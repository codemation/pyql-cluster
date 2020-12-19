async def run(server):
    import time
    import asyncio
    from fastapi import Request, Depends

    log = server.log

    @server.api_route('/cluster/{cluster}/table/{table}/recovery', methods=['POST'])
    async def cluster_table_sync_recovery(
        cluster: str, 
        table: str, 
        reqeust: Request,
        token: dict = Depends(server.verify_token),
    ): 
        return await cluster_table_sync_recovery(cluster, table,  request=await server.process_request(request))
    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_table_sync_recovery(cluster, table, **kw):
        """
        expects cluster uuid input for cluster, table string
        """
        return await table_sync_recovery(cluster, table, **kw)
    @server.trace
    async def table_sync_recovery(cluster, table, **kw):
        """
            run when all table-endpoints are in_sync=False
        """
        trace = kw['trace']
        #need to check quorum as all endpoints are currently in_sync = False for table
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        """
        if cluster == pyql:
            quorum_check, rc = cluster_quorum(trace=kw['trace'])
            if not quorum_check['quorum']['in_quorum'] == True:
                error = f"unable to perform while outOfQuorum - quorum {quorum_check}"
                return {"error": trace.error(error)}
        """
        quorum_check = kw['quorum']

        # Need to check all endpoints for the most up-to-date loaded table
        select = {'select': ['path', 'db_name', 'uuid'], 'where': {'cluster': cluster}}
        cluster_endpoints = await server.data['cluster'].tables['endpoints'].select(
            'path', 'db_name', 'uuid', 'token',
            where={'cluster': cluster}
        )
        latest = {'endpoint': None, 'last_mod_time': 0.0}
        trace.warning(f"cluster {cluster} endpoints {cluster_endpoints}")
        find_latest = {'select': ['last_mod_time'], 'where': {'table_name': table}}
        for endpoint in cluster_endpoints:
            if cluster == pyql and not endpoint['uuid'] in quorum_check['quorum']['nodes']['nodes']:
                trace.warning(f"endpoint {endpoint} is not in quorum, so assumed as dead")
                continue
            db_name = endpoint['db_name'] if cluster == pyql else 'pyql'
            pyql_tb_check, rc = await server.probe(
                f"http://{endpoint['path']}/db/{db_name}/table/pyql/select",
                method='POST',
                token=endpoint['token'],
                data=find_latest,
                session=await server.get_endpoint_sessions(endpoint['uuid'], **kw),
                timeout=2.0,
                **kw
            )
            trace(f"table_sync_recovery - checking last_mod_time on cluster {cluster} endpoint {endpoint}")
            if len(pyql_tb_check) > 0 and pyql_tb_check['data'][0]['last_mod_time'] > latest['last_mod_time']:
                latest['endpoint'] = endpoint['uuid']
                latest['last_mod_time'] = pyql_tb_check['data'][0]['last_mod_time']
        trace(f"table_sync_recovery latest endpoint is {latest['endpoint']}")
        update_set_in_sync = {
            'set': {'in_sync': True}, 
            'where': {
                'name': f"{latest['endpoint']}{table}"
                }
            }
        if cluster == pyql and table == 'state':
            #special case - cannot update in_sync True via clusterSvcName - still no in_sync endpoints
            for endpoint in cluster_endpoints:
                if not endpoint['uuid'] in quorum_check['quorum']['nodes']['nodes']:
                    trace.warning(f"table_sync_recovery - endpoint {endpoint} is not in quorum, so assumed as dead")
                    continue
                stateUpdate, rc = await server.probe(
                    f"http://{endpoint['path']}/db/cluster/table/state/update",
                    'POST',
                    update_set_in_sync,
                    token=endpoint['token'],
                    session=await server.get_endpoint_sessions(endpoint['uuid'], **kw),
                    timeout=2.0,
                    **kw
                )
        else:
            await server.cluster_table_change(pyql, 'state', 'update', update_set_in_sync, **kw)
            #cluster_table_update(pyql, 'state', update_set_in_sync)
        trace.warning(f"table_sync_recovery completed selecting an endpoint as in_sync -  {latest['endpoint']} - need to requeue job and resync remaining nodes")
        return {"message": trace("table_sync_recovery completed")}

    @server.api_route('/cluster/pyql/tablesync/check', methods=['POST'])
    async def cluster_tablesync_mgr(
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_tablesync_mgr( request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_tablesync_mgr(**kw):
        return await tablesync_mgr(action, trace=kw['trace'])
    @server.trace
    async def tablesync_mgr(**kw):
        """
            invoked regularly by cron or ondemand 

            creates jobs which bring a stale / new table endpoint into 'loaded' state
        """
        trace=kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']

        trace(f"starting")

        jobs_to_create = {}
        jobs = {}
        tables = await server.data['cluster'].tables['tables'].select('name', 'cluster')
        for table in tables:
            cluster = table['cluster']
            table_name = table['name']
            endpoints = await server.get_table_endpoints(cluster, table_name, **kw)
            if not len(endpoints['loaded'].keys()) > 0:
                trace.error(f"this should not happen, detected all endpoints for {cluster} {table_name} are not loaded")
                await table_sync_recovery(cluster, table_name, **kw)
                endpoints = await server.get_table_endpoints(cluster, table_name, **kw)

            new_or_stale_endpoints = endpoints['new']
            new_or_stale_endpoints.update(endpoints['stale'])
            trace(f"new_or_stale_endpoints detected: {new_or_stale_endpoints}")

            for endpoint in new_or_stale_endpoints:
                endpoint_path = new_or_stale_endpoints[endpoint]['path']
                if not cluster in jobs_to_create:
                    jobs_to_create[cluster] = {}
                if not table_name in jobs_to_create[table['cluster']]:
                    jobs_to_create[cluster][table_name] = []
                jobs_to_create[cluster][table_name].append({'endpoint': endpoint, 'path': endpoint_path})
        for cluster in jobs_to_create:
            jobs[cluster] = []
            for table in jobs_to_create[cluster]:
                # Add sync_table job for each table in cluster
                jobs[cluster].append({
                    'job': f'sync_table_{cluster}_{table}',
                    'job_type': 'syncjobs',
                    'action': 'table_sync_run',
                    'table': table, 
                    'table_paths': jobs_to_create[cluster][table],
                    'cluster': cluster,
                    'config': {
                        'cluster': cluster, 
                        'table': table, 
                        'job': f'sync_table_{cluster}_{table}'}
                    })
        
        for cluster in jobs:
            tables_in_jobs = [job['table'] for job in jobs[cluster]]
            order = [
                'data_to_txn_cluster',
                'state',
                'tables',
                'clusters', 
                'auth', 
                'endpoints', 
                'jobs'
            ]
            pyql_txn_tables = False
            for table in order:
                for job_table in tables_in_jobs:
                    if table in job_table:
                        pyql_txn_tables = True

            if cluster == pyql or pyql_txn_tables:
                #order = ['jobs', 'state', 'tables', 'clusters', 'auth', 'endpoints', 'transactions'] # 
                jobs_to_run_ordered = []
                ready_jobs = []
                while len(order) > 0:
                    #stateCheck = False
                    last_pop = None
                    for job in jobs[cluster]:
                        if len(order) == 0:
                            break
                        if order[0] in job['table']:
                            #if order[0] == 'state':
                            #    stateCheck = True
                            last_pop = order.pop(0)
                            jobs_to_run_ordered.append(job)
                            """
                            if last_pop == 'state':
                                for endpoint in job['tablePaths']:
                                    ready_jobs.append({
                                        'job': f"mark_ready_{endpoint['endpoint']}",
                                        'job_type': 'cluster',
                                        'action': 'update_cluster_ready',
                                        'config': {'ready': True, 'path': endpoint['path']}
                                    })
                            """
                    if last_pop == None:
                        order.pop(0)
                jobs_to_run_ordered = jobs_to_run_ordered + ready_jobs
                await server.wait_on_jobs(pyql, 0, jobs_to_run_ordered)
            else:
                for job in jobs[cluster]:
                    await jobs_add(job, trace=trace)
        trace.info(f"cluster_tablesync_mgr created {jobs} for outofSync endpoints")
        return {"jobs": jobs}
    server.clusterjobs['tablesync_mgr'] = tablesync_mgr

    
    @server.trace
    async def txn_table_sync(cluster, table, alive_endpoints, table_endpoints, **kw):
        """
        used to sync log type tables in cluster in following events:
        - new endpoint is added to pyql cluster and txn_cluster is not reached max endpoitns
        - existing endpoint re-joins pyql cluster - existing txn_cluster tables are stale
        - out of quorum endpoint now in quorum 
        1. Get dict of endpoints - stale / new
        2. check live-ness
        3. create table, if new
        4. Get Copy of a 'loaded' table
        5. Pause Ops to this table in Txn cluster
        6. Final Sync 
        7. Un-Pause & Resume ops
        """
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        loop = kw['loop']
        trace = kw['trace']

        pyql = await server.env['PYQL_UUID']
        pyql_under = '_'.join(pyql.split('-'))

        new_or_stale_endpoints = {}
        new_or_stale_endpoints.update(table_endpoints['new'])
        new_or_stale_endpoints.update(table_endpoints['stale'])

        is_paused = False

        trace(f"started - new_or_stale_endpoints: {new_or_stale_endpoints} - alive_endpoints: {alive_endpoints} ")

        ## create table 
        # create tables on new endpoints
        if len(table_endpoints['new']) > 0:
            table_config = None
            create_requests = []
            for _new_endpoint in table_endpoints['new']:
                if not _new_endpoint in alive_endpoints:
                    continue
                new_endpoint = table_endpoints['new'][_new_endpoint]
                
                # avoid pulling table config twice
                table_config = await server.cluster_table_config(
                    cluster, table, **kw
                    ) if table_config == None else table_config 
            
                # trigger table creation on new_endpoint
                db = new_endpoint['db_name']
                path = new_endpoint['path']
                epuuid = new_endpoint['uuid']

                create_requests.append(
                    server.rpc_endpoints[epuuid]['create_table'](db, table_config)
                )

            create_table_results = await asyncio.gather(*create_requests, return_exceptions=True)

            trace(f"{cluster} {table} create_table_results: {create_table_results}")

        if f'{pyql_under}_tables' in table:
            await server.table_pause(cluster, table, 'start', **kw)
            is_paused = True
            await asyncio.sleep(5)
        # get copy
        table_copy = await server.cluster_table_copy(cluster, table, copy_only=True, **kw)

        trace(f"{cluster} {table} - table_copy: - {table_copy}")

        if len(table_copy['table_copy']) > 0:
            # sync tables - pre cutover
            sync_requests = []

            for _endpoint in new_or_stale_endpoints:
                if not _endpoint in alive_endpoints:
                    continue
                endpoint = new_or_stale_endpoints[_endpoint]
        
                db = endpoint['db_name']
                path = endpoint['path']
                epuuid = endpoint['uuid']

                sync_requests.append(
                    server.rpc_endpoints[epuuid]['sync_table'](db, table, table_copy)
                )

            sync_table_results = await asyncio.gather(*sync_requests, return_exceptions=True)
        else:
            sync_table_results = {}
            for _endpoint in new_or_stale_endpoints:
                if not _endpoint in alive_endpoints:
                    continue
                endpoint = new_or_stale_endpoints[_endpoint]
                sync_table_results[endpoint['uuid']] = {'status': 200}
        trace(f"sync_table_results: {sync_table_results}")



        if len(table_copy['table_copy']) > 0:
            # pull changes 
            latest_timestamp = table_copy['table_copy'][-1]['timestamp']
            select_data = {
                'select': ['*'],
                'where': [
                    ['timestamp', '>', latest_timestamp]
                ]
            }
        else:
            select_data = None

        table_changes = await server.cluster_table_select(
            cluster,
            table,
            data=select_data,
            **kw
        )

        trace(f"{cluster} {table} - is_paused: {is_paused} - table_changes: - {len(table_changes['data'])}")

        while not is_paused or len(table_changes['data']) > 0:
            if not is_paused and len(table_changes['data']) < 50:
                await server.table_pause(cluster, table, 'start', **kw)
                is_paused = True
                trace(f"{cluster} {table} - starting pause as only {len(table_changes['data'])} changes - then sleeping")
                await asyncio.sleep(5)

                table_changes = await server.cluster_table_select(
                    cluster,
                    table,
                    data=select_data,
                    **kw
                )
            
            if len(table_changes['data']) == 0:
                continue
            sync_changes_requests = []
            for _endpoint in sync_table_results:
                if not sync_table_results[_endpoint]['status'] == 200:
                    continue
                endpoint = new_or_stale_endpoints[_endpoint]

                db = endpoint['db_name']
                path = endpoint['path']
                epuuid = endpoint['uuid']

                sync_changes_requests.append(
                    server.rpc_endpoints[epuuid]['insert'](db, table, params=table_changes['data'])
                )

            sync_changes_results = await asyncio.gather(*sync_changes_requests, return_exceptions=True)

            # check for new changes
            latest_timestamp = table_changes['data'][-1]['timestamp']
            select_data = {
                'select': ['*'],
                'where': [
                    ['timestamp', '>', latest_timestamp]
                ]
            }
            table_changes = await server.cluster_table_select(
                cluster,
                table,
                data=select_data,
                **kw
            )

            trace(f"{cluster} {table} - is_paused: {is_paused} - table_changes: - {len(table_changes['data'])}")

        else:
            sync_changes_results = {}
            for _endpoint in sync_table_results:
                endpoint = new_or_stale_endpoints[_endpoint]
                sync_changes_results[endpoint['uuid']] = {'status': 200}

        trace(f"sync_changes_results:  {sync_changes_results}")

        # mark endpoint loaded
        # mark table endpoint loaded
        state_update_results = []
        state_updates = []

        for endpoint in sync_changes_results:
            if not sync_changes_results[endpoint]['status'] == 200:
                continue
            if not f'{pyql_under}_state' in table:
                def get_state_change():
                    return server.cluster_table_change(
                        pyql,
                        'state',
                        'update',
                        {
                            'set': {
                                'state': 'loaded',
                                'info': {}
                                },
                            'where': {
                                'name': f"{endpoint}_{table}"
                                }
                        },
                        trace=trace,
                        force=True if f'{pyql_under}_tables' in table else False,
                        loop=loop
                    )
            else:
                async def get_state_change():
                    state_change = await server.cluster_table_change(
                        pyql,
                        'state',
                        'update',
                        {
                            'set': {
                                'state': 'loaded',
                                'info': {}
                                },
                            'where': {
                                'name': f"{endpoint}_{table}"
                                }
                        },
                        trace=trace,
                        force=True,
                        loop=loop
                    )
                    # check if this txn table is used by pyql state 
                    trace(f"pyql state txn table detected, waiting 5 sec then grabbing last txn table")
                    await asyncio.sleep(5)
                    state_endpoint = new_or_stale_endpoints[endpoint]
            
                    db = state_endpoint['db_name']
                    path = state_endpoint['path']
                    epuuid = state_endpoint['uuid']
                    token = state_endpoint['token']

                    latest_state_data = table_changes['data'] if len(table_changes['data']) > 0 else table_copy['table_copy']

                    latest_state_timestamp = latest_state_data[-1]['timestamp']
                    state_select_data = {
                        'select': ['*'],
                        'where': [
                            ['timestamp', '>', latest_state_timestamp]
                        ]
                    }
                    latest_state_changes = await server.cluster_table_select(
                        cluster,
                        table,
                        data=state_select_data,
                        exclude=epuuid, # Avoids use of this endpoint ( marked 'loaded' but not fully synced, yet) 
                        **kw
                    )
                    trace(f"lastest state change: {latest_state_changes}")

                    state_requests = []
                    if len(latest_state_changes['data']) > 0:
                        state_requests.append(
                            server.rpc_endpoints[epuuid]['insert'](db, table, params=latest_state_changes['data'])
                        )
                        state_change_result = await asyncio.gather(*state_requests, return_exceptions=True)

                        trace(f"state_change_result: - {state_change_result}")
            state_update_results.append(
                await get_state_change()
            )
        if f'{pyql_under}_tables' in table:
            trace(f"pyql tables txn table detected, waiting 5 sec before un-pausing table")
            await asyncio.sleep(5)
        # end cut-over
        await server.table_pause(cluster, table, 'stop', **kw)

        return {
            "sync_table_results": sync_table_results,
            "sync_changes_results": sync_changes_results, 
            "state_updates": {
                "state_update_results": state_update_results
            }
        }

    @server.trace
    async def pyql_state_sync_run(table, alive_endpoints, table_endpoints, **kw):
        """
        to be used when syncing pyql table ['state', 'tables', 'jobs']
        as logs are not used for syncing
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID']

        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        loop = kw['loop']
        
        # table should have been created for new endpoints already

        # issue / start cutover
        pause_table = await server.table_pause(pyql, table, 'start', **kw)
        trace(f"pause_table starting - {pause_table}")
        await asyncio.sleep(2)

        # for each created table, need to send a /db/database/table/sync 
        # which includes copy of latest table & last_txn_time

        new_or_stale_endpoints = {}
        new_or_stale_endpoints.update(table_endpoints['new'])
        new_or_stale_endpoints.update(table_endpoints['stale'])

        table_copy = None

        sync_requests = {} 
        for _endpoint in new_or_stale_endpoints:
            if not _endpoint in alive_endpoints:
                continue
            endpoint = new_or_stale_endpoints[_endpoint]
            
            # avoid pulling table copy twice
            if table_copy == None:
                table_copy = await server.cluster_table_copy(
                    pyql, 
                    table, 
                    copy_only=True,
                    **kw
                )
                # trigger table creation on new_endpoint
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']

            sync_requests.append(
                server.rpc_endpoints[epuuid]['sync_table'](db, table, table_copy)
            )

            if table == 'state':
                # Allowing only 1 state table sync per job, to avoid state mismatches
                break
        sync_table_results = await asyncio.gather(*sync_requests, return_exceptions=True)

        trace(f"sync_table_results: {sync_table_results}")

        # mark loaded
        mark_loaded = []
        for endpoint in sync_table_results:
            if not sync_table_results[endpoint]['status'] == 200:
                continue
            set_loaded = {
                'set': {
                    'state': 'loaded',
                    'info': {}
                    },
                'where': {
                    'name': f"{endpoint}_{table}"
                    }
            }
            mark_loaded.append(
                server.cluster_table_change(
                    pyql,
                    'state',
                    'update',
                    set_loaded,
                    force=True,
                    trace=trace,
                    loop=loop
                )
            )
            # update stale - 'state' endpoint 
            if table == 'state':
                stale_state_update = {}
                _endpoint = new_or_stale_endpoints[endpoint]
                db = _endpoint['db_name']
                path = _endpoint['path']
                epuuid = _endpoint['uuid']

                mark_loaded.append(
                    server.rcp_endpoints[epuuid]['update'](db, table, set_loaded)
                )

        mark_loaded_results = await asyncio.gather(
            *mark_loaded,
            return_exceptions=True
        )
        trace(f"mark_loaded_results: {mark_loaded_results}")
        

        # post sync - unpause
        unpause_table = await server.table_pause(pyql, table, 'stop', **kw)
        trace(f"unpause_table - {unpause_table}")
        return {
            "sync_table_results": sync_table_results,
            "mark_loaded": mark_loaded_results
        }

    @server.trace
    async def table_sync_run(cluster=None, table=None, config=None, job=None, **kw):
        """
        called by a job created by tablesync_mgr

        runs in following conditions:
        - new endpoint joins a cluster which creates 'new' tables matching existing 
        - existing endpoint re-joins cluster (from instance restart) if endpoint is 'stale'
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID']

        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        loop = kw['loop']

        sync_results = {}
        if cluster == None or table == None or job == None:
            cluster, table, job = (
                config['cluster'],
                config['table'],
                config['job']
            )

        trace(f"starting for cluster: {cluster} table {table}")

        # get list of table endpoints that are not 'loaded'
        table_endpoints = await server.get_table_endpoints(cluster, table, **kw)
        cluster_type = table_endpoints['cluster_type']

        new_or_stale_endpoints = table_endpoints['new']
        new_or_stale_endpoints.update(table_endpoints['stale'])

        all_table_endpoints = {}

        for state in ['loaded', 'stale', 'new']:
            all_table_endpoints.update(table_endpoints[state])

        # verify endpoints are alive
        check_alive_endpoints = await server.get_alive_endpoints(
            all_table_endpoints,
            **kw
        )
        alive_endpoints = []
        for endpoint in check_alive_endpoints:
            alive_endpoints.append(endpoint)
        
        # Log Cluster Tables Workflow 
        if cluster_type == 'log':
            return await txn_table_sync(
                cluster, 
                table, 
                alive_endpoints, 
                table_endpoints,
                **kw
            )

        # create tables on new endpoints
        if len(table_endpoints['new']) > 0:
            table_config = None
            create_requests = []
            for _new_endpoint in table_endpoints['new']:
                if not _new_endpoint in alive_endpoints:
                    continue
                new_endpoint = table_endpoints['new'][_new_endpoint]
                
                # avoid pulling table config twice
                table_config = await server.cluster_table_config(
                    cluster, table, **kw
                    ) if table_config == None else table_config 
            
                # trigger table creation on new_endpoint
                db = new_endpoint['db_name']
                path = new_endpoint['path']
                epuuid = new_endpoint['uuid']
                token = new_endpoint['token']
                
                async def create_table():
                    try:
                        return {
                            epuuid: await server.rpc_endpoints[epuuid]['create_table'](
                                db,
                                table_config[table]
                            )
                        }
                    except Exception as e:
                        return {
                            epuuid: {
                                'error': trace.exception(f"error with create_table")
                            }
                        }
                create_requests.append(
                    create_table()
                )
                
            create_table_results = await server.gather_items(create_requests)
            trace(f"{cluster} {table} {job} create_table_results: {create_table_results}")

            for endpoint in create_table_results:
                if 'error' in create_table_results[endpoint]:
                    del table_endpoints['new'][endpoint]


            if cluster == pyql and table in ['jobs', 'tables', 'state']:
                return await pyql_state_sync_run(
                    table, 
                    alive_endpoints,
                    table_endpoints,
                    **kw
                )

            # for each created table, need to send a /db/database/table/sync 
            # which includes copy of latest table & last_txn_time
            table_copy = None

            sync_requests = [] 
            for _new_endpoint in table_endpoints['new']:
                if not _new_endpoint in alive_endpoints:
                    continue
                new_endpoint = table_endpoints['new'][_new_endpoint]
                
                # avoid pulling table config twice
                if table_copy == None:
                    table_copy = await server.cluster_table_copy(
                        cluster, 
                        table, 
                        **kw
                    )
                    # trigger table creation on new_endpoint
                db = new_endpoint['db_name']
                epuuid = new_endpoint['uuid']

                sync_requests.append(
                    server.rpc_endpoints[epuuid]['sync_table'](db, table, table_copy)
                )

            sync_table_results = await asyncio.gather(*sync_requests, return_exceptions=True)
        # trigger flush ( first sync for new ) on stale & new endpoints
        # for all endpoints trigger /flush


        # gather flush config
        txn_cluster = await server.data['cluster'].tables['data_to_txn_cluster'].select(
            'txn_cluster_id',
            where={'data_cluster_id': cluster}
        )
        txn_cluster_id = txn_cluster[0]['txn_cluster_id']

        tx_table = '_'.join(f"txn_{cluster}_{table}".split('-'))

        # create limited use token
        limited_use_token = await server.create_auth_token(
            cluster, # id
            time.time() + 300,
            'cluster',
            extra_data={
                'cluster_allowed': txn_cluster_id,
                'table_allowed': tx_table
            }
        )

        flush_config = {
            "tx_cluster_endpoint": await server.get_random_alive_table_endpoint(txn_cluster_id, tx_table, **kw),
            "tx_cluster_id": txn_cluster_id,
            "tx_table": tx_table
        }


        # create signal endpoint config

        flush_requests = []
        for _endpoint in new_or_stale_endpoints:
            if not _endpoint in alive_endpoints:
                continue
            endpoint = new_or_stale_endpoints[_endpoint]
                # trigger table creation on new_endpoint
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']

            async def flush_trigger():
                try:
                    return {
                        epuuid: server.rpc_endpoints[epuuid]['table_flush_trigger'](
                            db, table, flush_config
                            )
                        }
                except Exception as e:
                    return {
                        epuuid: {'error': trace.exception(f"error with table_flush_trigger")}
                    }

            flush_requests.append(
                flush_trigger()
            )
            
        flush_results = await server.gather_items(flush_requests)
        trace(f"{cluster} {table} {job} flush_results: {flush_results}")

        # mark table endpoint loaded
        state_updates = []
        state_update_results = []
        for endpoint in flush_results:
            if 'error' in flush_requests[endpoint]:
                continue
            state_update_results.append(
                await server.cluster_table_change(
                    pyql,
                    'state',
                    'update',
                    {
                        'set': {
                            'state': 'loaded',
                            'info': {}
                            },
                        'where': {
                            'name': f"{endpoint}_{table}"
                            }
                    },
                    trace=trace,
                    loop=loop
                )
            )
        #state_update_results = await asyncio.gather(*state_updates, loop=loop)
        trace(f"{cluster} {table} {job} state_update_results: {state_update_results}")
        await asyncio.sleep(10)

        for _endpoint in new_or_stale_endpoints:
            if not _endpoint in alive_endpoints:
                continue
            endpoint = new_or_stale_endpoints[_endpoint]
                # trigger table creation on new_endpoint
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']
            if cluster == pyql:
                trace(f"checking if pyql endpoint can be marked ready=True")
                ready = True
                for table_state in await server.data['cluster'].tables['state'].select(
                    'state', 'name', 
                    where={"uuid": epuuid}
                    ):
                    if table_state['state'] in ['new', 'stale']:
                        trace(f"{table_state['name']} is still {table_state['state']}, cannot mark endpoint ready")
                        ready=False
                        break
                # no tables for endpoint are in_sync false - mark endpoint ready = True
                if ready:
                    await server.update_cluster_ready(path=path, ready=True, **kw)

        return {"state_update_results": state_update_results, "flush_results": flush_results}

    server.clusterjobs['table_sync_run'] = table_sync_run