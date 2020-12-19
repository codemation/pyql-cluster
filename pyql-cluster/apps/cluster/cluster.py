async def run(server):

    import asyncio
    import time, random
    from random import randrange

    import json, os
    from collections import Counter

    from typing import Union, Optional, List, Dict
    from fastapi import Request, Depends
    from pydantic import BaseModel

    
    from apps.cluster import tracer

    from apps.cluster.bootstrap import run as bootstrap
    from apps.cluster.jobs import run as jobs_setup
    from apps.cluster.join import run as join_setup
    from apps.cluster.quorum import run as quorum_setup
    from apps.cluster.rbac import run as rbac_setup
    from apps.cluster.sessions import run as session_setup
    from apps.cluster.tablesync import run as tablesync_setup
    from apps.cluster.transactions import run as txn_setup

    log = server.log
    server.trace = tracer.get_tracer(log)

    os.environ['PYQL_ENDPOINT'] = server.PYQL_NODE_ID
    os.environ['HOSTNAME'] = '-'.join(os.environ['PYQL_NODE'].split('.'))

    # used to store links to functions called by jobs
    server.clusterjobs = {}

    # setup methods used for resource verification    
    await rbac_setup(server)
    await quorum_setup(server)

    # setup auth apps which require server.state_and_quorum_check
    server.auth_post_cluster_setup(server)

    await bootstrap(server)

    # job endpoints and logic
    await jobs_setup(server)

    await join_setup(server)

    await session_setup(server)

    await tablesync_setup(server)

    await txn_setup(server)


    class PyqlUuid(BaseModel):
        PYQL_UUID: str

    class Insert(BaseModel):
        column1: str = 'value1'
        column2: str = 'value2'

    class Query(BaseModel):
        select: List[str] = ['*']
        where: Optional[dict] = {'column': 'value'}

    class Update(BaseModel):
        set: dict = {'col': 'val'}
        where: dict = {'col': 'val'}

    class KeyUpdate(BaseModel):
        column1: str = 'value1'
        column2: str = 'value2'

    class Select(BaseModel):
        selection: List[str] = ['col1', 'col2', 'col3']
        where: Optional[dict] = {'col1': 'val'}

    class Delete(BaseModel):
        where: dict = {'col': 'val'}

    @server.api_route('/pyql/setup', methods=['POST'])
    async def cluster_set_pyql_id_api(pyql_id: PyqlUuid, request: Request):
        return await cluster_set_pyql_id(dict(pyql_id),  request=await server.process_request(request))

    @server.is_authenticated('local')
    @server.trace
    async def cluster_set_pyql_id(pyql_id, **kw):
        return await set_pyql_id(pyql_id)

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def set_pyql_id(pyql_id, **kw):
        await server.env.set_item('PYQL_UUID', pyql_id['PYQL_UUID'])
        return {'message': log.warning(f"updated PYQL_UUID with {pyql_id['PYQL_UUID']}")}
    
    #No auth should be required 
    @server.api_route('/pyql/node')
    async def cluster_node():
        """
            returns node-id - to be used by workers instead of relying on pod ip:
        """
        return await node()

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def node():
        log.warning(f"get NODE_ID called {server.PYQL_NODE_ID}")
        return {"uuid": server.PYQL_NODE_ID}

    @server.trace
    async def cluster_endpoint_delete(cluster=None, endpoint=None, config=None, **kw):
        """
        This function is important to be used when node(s) goes down as the order in which
        a node comes up cannot always be guaranteed, so a node may come up with a different
        ip address
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        if not config == None:
            cluster, endpoint = config['cluster'], config['endpoint']
        trace.error(f"cluster_endpoint_delete called for cluster - {cluster}, endpoint - {endpoint}")
        delete_where = {'where': {'uuid': endpoint, 'cluster': cluster}}
        results = {}
        results['state'] = await cluster_table_change(pyql, 'state', 'delete', delete_where, **kw)
        results['endpoints'] = await cluster_table_change(pyql, 'endpoints', 'delete', delete_where, **kw)
        return {"message": trace(f"deleted {endpoint} successfully - results {results}")}
    server.clusterjobs['cluster_endpoint_delete'] = cluster_endpoint_delete

    @server.trace
    async def get_random_alive_table_endpoint(cluster_id: str, table: str, **kw):
        table_endpoints = await get_table_endpoints(cluster_id, table, **kw)
        alive_endpoints = await get_alive_endpoints(table_endpoints['loaded'], **kw)
        return random.choice(alive_endpoints)
    server.get_random_alive_table_endpoint = get_random_alive_table_endpoint


    @server.trace
    async def get_alive_endpoints(endpoints, timeout=2.0, **kw):
        trace = kw['trace']
        loop = server.event_loop if not 'loop' in kw else kw['loop']

        trace(f"starting - checking endpoints: {endpoints}")

        endpoint_check = []
        for _endpoint in endpoints:
            endpoint = _endpoint if isinstance(endpoints, list) else endpoints[_endpoint]
            if not endpoint['uuid'] in server.rpc_endpoints:
                trace(f"endpoint: {endpoint['uuid']} not found in rpc_endpoints")
                continue
            trace(f"checking {endpoint}")
            endpoint_check.append(
                server.rpc_endpoints[endpoint['uuid']]['node']()
            )
        ep_results = await asyncio.gather(*endpoint_check, return_exceptions=True)
        trace.warning(f"get_alive_endpoints - {ep_results}")
        ep_results = [ep['uuid'] for ep in ep_results]
        trace.warning(f"get_alive_endpoints finished - {ep_results}")
        return ep_results
    server.get_alive_endpoints = get_alive_endpoints


    async def gather_items(list_of_coroutines: list):
        """
        converts a list of gathered

        list_of_coroutines:
             should contain initialized coroutines which return a key-value pair
        """
        results_list = await asyncio.gather(
            *list_of_coroutines,
            return_exceptions=True
        )
        results = {}
        for result in results_list:
            for k,v in result.items():
                results[k] = v
        return results
    server.gather_items = gather_items

   
    @server.api_route('/cluster/{cluster}/table/{table}/path')
    async def cluster_get_db_table_path(
        cluster: str, 
        table: str, 
        request: Request, 
        token: dict = Depends(server.verify_token)
    ):
        return await get_db_table_path(cluster, table,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def get_db_table_path(cluster, table, **kw):
        paths = {'loaded': {}, 'stale': {}, 'new': {}}
        table_endpoints = await get_table_endpoints(cluster, table, trace=kw['trace'])
        tb = await get_table_info(cluster, table, table_endpoints, trace=kw['trace'])
        for pType in paths:
            for endpoint in table_endpoints[pType]:
                #dbName = get_db_name(cluster, endpoint)
                dbName = tb['endpoints'][f'{endpoint}{table}']['db_name']
                paths[pType][endpoint] = tb['endpoints'][f'{endpoint}{table}']['path']
        return paths

    @server.api_route('/cluster/{cluster}/table/{table}/endpoints')
    async def cluster_get_table_endpoints_api(
        cluster: str, 
        table: str, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_get_table_endpoints(cluster, table,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_get_table_endpoints(cluster, table, **kw):
        cluster_name = kw['request'].__dict__.get('cluster_name')
        return await get_table_endpoints(cluster, table,cluster_name=cluster_name, **kw)

    @server.trace
    async def get_table_endpoints(cluster, table, cluster_name=None, **kw):
        trace = kw['trace']

        exclude = [] if not 'exclude' in kw else [kw['exclude']] 

        table_endpoints = {'loaded': {}, 'new': {}, 'stale': {}}

        # Get cluster Name - coro
        _cluster = server.data['cluster'].tables['clusters'].select(
            'name',
            'type',
            where={'id': cluster}
        )

        # Get endpoints in cluster - coro
        endpoints = server.data['cluster'].tables['endpoints'].select(
            '*', 
            join={
                'state': {
                    'endpoints.uuid': 'state.uuid', 
                    'endpoints.cluster': 'state.cluster'}
            }, 
            where={
                'state.cluster': cluster, 
                'state.table_name': table
            }
        )

        # Run Coros
        _cluster, endpoints = await asyncio.gather(
            _cluster,
            endpoints
        )
        _cluster = _cluster[0]

        # process {"<table>.<column>": <value>} into {"<column>": <value>} 
        endpoints_key_split = []
        for endpoint in endpoints:
            if endpoint['endpoints.uuid'] in exclude:
                trace(f"endpoint {endpoint} excluded")
                continue
            renamed = {}
            for k,v in endpoint.items():
                renamed[k.split('.')[1]] = v
            endpoints_key_split.append(renamed)

        for endpoint in endpoints_key_split:
            state = endpoint['state']
            table_endpoints[state][endpoint['uuid']] = endpoint
        table_endpoints['cluster_name'] = _cluster['name']
        table_endpoints['cluster_type'] = _cluster['type']
        trace.warning(f"result {table_endpoints}")
        return table_endpoints
    server.get_table_endpoints = get_table_endpoints

    @server.trace
    def get_endpoint_url(path, action, **kw):
        trace = kw['trace']
        cache_path = '/cache/'.join(path.split('/table/'))
        if 'commit' in kw or 'cancel' in kw:
            action = 'commit' if 'commit' in kw else 'cancel'
            return trace(f'{cache_path}/txn/{action}')
        if 'cache' in kw:
            return trace(f'{cache_path}/{action}/{kw["cache"]}')
        else:
            return trace(f'{path}/{action}')

    @server.trace
    async def get_table_info(cluster, table, endpoints, **kw):
        trace = kw['trace']

        tb = await server.data['cluster'].tables['tables'].select(
            '*',
            where={
                'id': f"{cluster}_{table}"
            }
        )
        if len(tb) == 0:
            server.http_exception(500, trace(f"no tables in cluster {cluster} with name {table} - found: {tables}"))
        tb = tb[0]
        trace(f"get_table_info_get_tables {tb}")
        tb['endpoints'] = {}
        for state in ['loaded', 'stale', 'new']:
            for endpoint in endpoints[state]:
                path = endpoints[state][endpoint]['path']
                db = endpoints[state][endpoint]['db_name']
                name = endpoints[state][endpoint]['name']
                tb['endpoints'][name] = endpoints[state][endpoint]
                tb['endpoints'][name]['path'] = f"http://{path}/db/{db}/table/{tb['name']}"
        trace(f"completed {tb}")
        return tb
        

    @server.trace
    async def cluster_table_read(cluster, table, data, **kw):
        """
        # Reads
        1. Check 'table shard cluster' associated with cluster
        2. Flush changes, to target endpoint, if any
        3. Read & Return 
        """
        trace = kw['trace']
        _txn = {action: request_data}
        trace(f"called for {_txn}")
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

    @server.trace
    async def pyql_state_tables_change(table, action, request_data, force=False, **kw):
        """
        Meant to be called for pyql cluster - tables ['tables', 'state', 'jobs']
        as logs will not be generated for these tables, but applied ASAP 
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

        trace(f"started for pyql table {table} action: {action} - request_data: {request_data}")

        if not force:
            # check if table is paused
            cur_wait, max_wait = 0.01, 10.0
            while cur_wait < max_wait:
                pause_check = await server.data['cluster'].tables['tables'].select(
                    'is_paused',
                    where={
                        'id': f'{pyql}_{table}'
                    }
                )
                if pause_check[0]['is_paused'] == False:
                    break
                # wait an try again
                await asyncio.sleep(0.01)
                cur_wait+=0.01
            else:
                return {"error": trace.error(f"timeout reached waiting for {table} to un-pause")}

        table_endpoints = await get_table_endpoints(pyql, table, **kw)

        state_change_requests = {}
        state_change_requests = []

        for endpoint in table_endpoints['loaded']:
            _endpoint = table_endpoints['loaded'][endpoint]
            path = _endpoint['path']
            db = _endpoint['db_name']
            token = _endpoint['token']
            epuuid = _endpoint['uuid']

            async def state_change():
                try:
                    if epuuid == server.PYQL_NODE_ID:
                        return {
                            epuuid: await server.actions[action](
                                db,
                                table,
                                params=request_data
                            )
                        }
                    return {
                        epuuid: await server.rpc_endpoints[epuuid][action](
                            db,
                            table,
                            params=request_data
                        )
                    }
                except Exception as e:
                    return {
                        epuuid: {'error': log.exception(f"error with state_change - {repr(e)}")}
                    }
            state_change_requests.append(state_change())

            """
            state_change_requests[endpoint] = {
                'path': f"http://{path}/db/{db}/table/{table}/{action}",
                'data': request_data,
                'timeout': 2.0,
                'headers': await server.get_auth_http_headers('remote', token=token),
                'session': await server.get_endpoint_sessions(epuuid, **kw)
            }
            """
        state_change_results = await gather_items(state_change_requests)

        status = {'success': [], 'fail': []}
        for _endpoint in state_change_results:
            endpoint = table_endpoints['loaded'][_endpoint]
            if not 'error' in state_change_results[_endpoint]:
                status['success'].append(endpoint)
                continue
            status['fail'].append(endpoint)

        trace(f"state_change_results status: {status}")

        if len(status['success']) == 0:
            return {
                'error': trace.error("all endpoints failed"),
                'status': status
            }
       
        for fail_endpoint in status['fail']:
            mark_stale = {
                "set": {
                    'state': 'stale',
                    'info': {
                        'stale reason': f'{table} {action} failed',
                        'operation': trace.get_root_operation(),
                        'node': server.PYQL_NODE_ID
                        }
                },
                'where': {
                    'name': f"{fail_endpoint['uuid']}_{table}"
                }
            }

            if not table == 'state':
                trace(f"marking fail_endpoint {fail_endpoint['uuid']} stale")
                await pyql_state_tables_change(
                    'state',
                    'update',
                    mark_stale,
                    **kw
                )
                continue

            ## state table ##
            #state_mark_stale = {}
            state_mark_stale = []
            for endpoint in status['success']:
                path = endpoint['path']
                db = endpoint['db_name']
                token = endpoint['token']
                epuuid = endpoint['uuid']

                async def set_state_stale():
                    try:
                        if epuuid == server.PYQL_NODE_ID:
                            return {
                                epuuid: await server.actions['update'](
                                    db,
                                    'state',
                                    params=mark_stale
                                )
                            }
                        return {
                            epuuid: await server.rpc_endpoints[epuuid]['update'](
                                db,
                                'state',
                                params=mark_stale
                            )
                        }
                    except Exception as e:
                        return {
                            epuuid: {'error': log.exception(f"error with state_change - {repr(e)}")}
                        }
                state_mark_stale.append(set_state_stale())
                """
                state_mark_stale[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/state/update",
                    'data': mark_stale,
                    'timeout': 2.0,
                    'headers': await server.get_auth_http_headers('remote', token=token),
                    'session': await server.get_endpoint_sessions(epuuid, **kw)
                }
                """
            state_mark_stale_results = await gather_items(state_mark_stale)
            trace(f"state_mark_stale_results: {state_mark_stale_results}")
        return {
            "state_change_results": state_change_results, 
            "status": status

        }
    @server.trace
    async def cluster_table_change(cluster, table, action, request_data, **kw):
        """
        called for table modifications 
        action: insert, update, delete 
        1. Check 'table shard cluster' associated with  cluster
        2. Write txn to 'table shard cluster'
        3. Return Success 

        Requirements:
        - Node processing request must be in_quorum
        - Node triggering table_change  
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']

        if cluster == pyql and table in ['jobs', 'state', 'tables']:
            return await pyql_state_tables_change(
                table,
                action,
                request_data, 
                **kw
            )

        _txn = {action: request_data}
        trace(f"called for {_txn}")
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

        # Check 'table txn log cluster' associated with  cluster
        txn_time = float(time.time())
        txn_cluster = await server.data['cluster'].tables['data_to_txn_cluster'].select(
            'txn_cluster_id',
            'txn_cluster_name',
            where={'data_cluster_id': cluster}
        )
        txn_cluster_id = txn_cluster[0]['txn_cluster_id']
        txn_cluster_name = txn_cluster[0]['txn_cluster_name']
        txn = {
            "timestamp": time.time(),
            "txn": _txn
        }
        cluster_id_underscored = (
            '_'.join(cluster.split('-'))
        )

        # Create Task to signal endpoints 
        @server.trace
        async def signal_table_endpoints(**kw):
            trace = kw['trace']
            trace("starting")
            loop = asyncio.get_running_loop()
            kw['loop'] = loop
            table_endpoints = await get_table_endpoints(cluster, table, loop=loop)
            flush_requests = {}
            flush_tasks = []
            flush_requests_results = {}
            for endpoint in table_endpoints['loaded']:
                _endpoint = table_endpoints['loaded'][endpoint]
                path = _endpoint['path']
                db = _endpoint['db_name']
                token = _endpoint['token']
                epuuid = _endpoint['uuid']

                tx_table = '_'.join(f"txn_{cluster}_{table}".split('-'))
                
                op = 'flush'

                flush_config = {
                    "tx_cluster_endpoint": await get_random_alive_table_endpoint(txn_cluster_id, tx_table, **kw),
                    "tx_cluster_id": txn_cluster_id,
                    "tx_table": tx_table
                }
                if epuuid == server.PYQL_NODE_ID:
                    async def local_flush():
                        flush_requests_results[endpoint] = {
                            'status': 200,
                            'content': await server.table_flush_trigger(
                                db,
                                table,
                                flush_config
                            )
                        }
                    flush_tasks.append(local_flush())
                    continue

                flush_tasks.append(
                    server.rpc_endpoints[epuuid]['table_flush_trigger'](
                        db,
                        table,
                        flush_config
                    )
                )

            flush_tasks_results = await asyncio.gather(
                *flush_tasks, 
                return_exceptions=True
            )
            trace(f"flush_tasks_results: {flush_tasks_results}")
            
            # handle flush trigger failures by marking stale
            flush_fail_mark_stale = []
            for endpoint in flush_requests_results:
                if flush_requests_results[endpoint]['status'] == 200:
                    continue
                state_data = {
                    "set": {
                        "state": 'stale',
                        'info': {
                            'stale reason': 'flush trigger failed',
                            'operation': trace.get_root_operation(),
                            'node': server.PYQL_NODE_ID
                            }
                    },
                    "where": {
                        "name": f"{endpoint}_{table}"
                    }
                }
                flush_fail_mark_stale.append(
                    cluster_table_change(
                            pyql,
                            'state',
                            'update',
                            state_data,
                            **kw
                        )
                )
            if len(flush_fail_mark_stale) > 0:
                mark_stale_results = await asyncio.gather(
                    *flush_fail_mark_stale, 
                    loop=loop, 
                    return_exceptions=True
                )
                trace(f"flush trigger failure(s) detected, marking failed endpoint(s) stale - result {mark_stale_results}")

            return trace(f"flush_requests_results: {flush_requests_results}")

        result = await write_to_txn_logs(
            txn_cluster_id,
            f"txn_{cluster_id_underscored}_{table}",
            txn,
            **kw
        )

        await server.txn_signals.put(
            (
                signal_table_endpoints(**kw), # to be awaited by txn_signal workers
                signal_table_endpoints, # to be retried, if first coro fails
                kw # to be passed into any retries
            )
        )

        return {"result": trace("finished")}
    server.cluster_table_change = cluster_table_change
        
    @server.trace
    async def write_to_txn_logs(log_cluster, log_table, txn, force=False, **kw):
        trace = kw['trace']
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        pyql_under = '_'.join(pyql.split('-'))

        stale_state_log_table = False if not 'stale_state_log_table' in kw else True

        if not force:
            # check if table is paused
            cur_wait, max_wait = 0.01, 10.0
            while cur_wait < max_wait:
                pause_check = await server.data['cluster'].tables['tables'].select(
                    'is_paused',
                    where={
                        'id': f'{log_cluster}_{log_table}'
                    }
                )
                if pause_check[0]['is_paused'] == False:
                    break
                # wait an try again
                await asyncio.sleep(0.01)
                cur_wait+=0.01
            else:
                return {"error": trace.error(f"timeout reached waiting for {log_table} to un-pause")}

        table_endpoints = await get_table_endpoints(
            log_cluster, 
            log_table, 
            **kw
        )
        #log_inserts = {}
        log_inserts = []
        log_insert_results = {}
        for endpoint in table_endpoints['loaded']:
            log_endpoint = table_endpoints['loaded'][endpoint]
            token = log_endpoint['token']
            db = log_endpoint['db_name']
            path = log_endpoint['path']
            ep_uuid = log_endpoint['uuid']
            
            async def log_insert():
                trace(f"starting log_insert for txn {txn}")
                try:
                    if ep_uuid == server.PYQL_NODE_ID:
                        trace(f"log_insert: LOCAL")
                        return {
                            ep_uuid: await server.actions['insert'](
                                db,
                                log_table,
                                txn
                            )
                        }
                    trace(f"log_insert: REMOTE")
                    return {
                        ep_uuid: await server.rpc_endpoints[ep_uuid]['insert'](
                            db, 
                            log_table, 
                            txn
                        )
                    }
                except Exception as e:
                    return {ep_uuid: {'error': trace.error(f"error with log_insert")}}

            log_inserts.append(log_insert())

        #log_insert_results = await asyncio.gather(*log_inserts)

        log_insert_results = await gather_items(log_inserts)

        trace(f"log_insert_results: {log_insert_results}")

        pass_fail = Counter({'pass': 0, 'fail': 0})
        log_state_out_of_sync = []
        for endpoint in log_insert_results:
            if 'error' in log_insert_results[endpoint]:
                pass_fail['fail']+=1
                if not stale_state_log_table:
                    state_data = {
                        "set": {
                            "state": 'stale',
                            'info': {
                                'stale reason': 'log insertion failed',
                                'operation': trace.get_root_operation(),
                                'node': server.PYQL_NODE_ID
                                }
                        },
                        "where": {
                            "name": f"{endpoint}_{log_table}"
                        }
                    }
                    if log_table == f'txn_{pyql_under}_state':
                        kw['stale_state_log_table'] = True
                    log_state_out_of_sync.append(
                        await cluster_table_change(
                                pyql,
                                'state',
                                'update',
                                state_data,
                                **kw
                            )
                    )
            else:
                pass_fail['pass']+=1

        # mark failures out_of_sync if sucesses & failures exist
        if pass_fail['fail'] > 0 and pass_fail['success'] > 0:
            trace(f"log insertion failure detected, marking failed endpoints stale - result {log_state_out_of_sync}")

        return {'results': log_insert_results, 'pass_fail': pass_fail}

    @server.trace
    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def table_select(cluster, table, **kw):
        return await endpoint_probe(cluster, table, 'select', **kw)
    server.cluster_table_select = table_select

    @server.trace
    async def get_random_table_endpoint(cluster, table, quorum=None, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']

        endpoints = await get_table_endpoints(cluster, table, **kw)
        endpoints = endpoints['loaded']
        loaded_endpoints = [ep for ep in endpoints]

        while len(loaded_endpoints) > 0: 
            if server.PYQL_NODE_ID in loaded_endpoints:
                endpoint_choice = loaded_endpoints.pop(loaded_endpoints.index(server.PYQL_NODE_ID))
            else:
                if len(loaded_endpoints) > 1:
                    endpoint_choice = loaded_endpoints.pop(randrange(len(loaded_endpoints)))
                else:
                    endpoint_choice = loaded_endpoints.pop(0)
            if not quorum == None and cluster == pyql:
                if not endpoints[endpoint_choice]['uuid'] in quorum['quorum']['nodes']:
                    trace.warning(f"get_random_table_endpoint skipped pyql endpoint {endpoints[endpoint_choice]} as not in quorum: {quorum}")
                    if len(loaded_endpoints) == 0 and table == 'jobs':
                        #await pyql_reset_jobs_table(**kw)   
                        endpoints = await get_table_endpoints(cluster, table, **kw)
                        endpoints = endpoints['loaded']
                        loaded_endpoints = [ep for ep in endpoints]
                    continue
            yield endpoints[endpoint_choice]
        yield None
    #table_select(pyql, 'jobs', data=job_select, method='POST', **kw)    
    @server.trace
    async def endpoint_probe(cluster, table, path='', data=None, timeout=1.0, quorum=None, **kw):
        trace = kw['trace']
        request = kw['request'] if 'request' in kw else None
        errors = []

        if data == None:
            if not request == None:
                method = request.method if not 'method' in kw else kw['method']
            else:
                method = 'GET'
        else:
            if request == None or not 'method' in kw:   
                method = 'POST'
                path = '/select' if path == '' else path
            else:
                method = request.method if not request == None else kw['method']

        if not 'method' in kw:
            kw['method'] = method if request == None else request.method
        
        if method in ['POST', 'PUT'] and data == None:
            server.http_exception(400, trace.error("expected json input for request"))
        async for endpoint in get_random_table_endpoint(cluster, table, quorum, **kw):
            if endpoint == None:
                server.http_exception(
                    500, 
                    trace(f"no in_sync endpoints in cluster {cluster} table {table} or all failed - errors {errors}")
                )
            try:
                if path == '' or path == 'select': # table select
                    if endpoint['uuid'] == server.PYQL_NODE_ID:
                        # local node, just use local select
                        return await server.actions['select'](endpoint['db_name'], table, params=data, method=method)
                    return await server.rpc_endpoints[endpoint['uuid']][path](endpoint['db_name'], table, params=data)
                
                if path == 'config': # table config pull
                    if endpoint['uuid'] == server.PYQL_NODE_ID:
                        return await server.get_table_config(endpoint['db_name'], table)
                    return await server.rpc_endpoints[endpoint['uuid']]['get_table_config'](endpoint['db_name'], table)

                # /key
                if endpoint['uuid'] == server.PYQL_NODE_ID:
                    return await server.actions['select_key'](endpoint['db_name'], table, path)
                return await server.rpc_endpoints[endpoint['uuid']]['table_key'](endpoint['db_name'], table, path)
            except Exception as e:
                errors.append({endpoint['name']: trace.exception(f"exception encountered with {endpoint}")})
                continue
    
    @server.api_route('/cluster/{cluster}/table/{table}')
    async def cluster_table_get(
        cluster: str, 
        table:str, 
        request: Request, 
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_auth(cluster, table, request=await server.process_request(request))

    @server.api_route('/cluster/{cluster}/table/{table}', methods=['PUT', 'POST'])
    async def cluster_table_add(
        cluster: str, 
        table:str, 
        request: Request, 
        data: Insert,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_auth(cluster, table, data=data,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_auth(cluster, table, **kw):
        request = kw['request']
        if request.method == 'GET':
            return await endpoint_probe(cluster, table, **kw)
        return table_insert(cluster, table, **kw)


    @server.api_route('/cluster/{cluster}/table/{table}/create', methods=['POST'])
    async def cluster_table_create_api(
        cluster: str, 
        table: str, 
        request: Request, 
        config: dict,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_create_auth(
            cluster, 
            table, 
            request=await server.process_request(request),
            config=config
        )
    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_create_auth(cluster: str, table: str, config: dict, **kw):
        return await cluster_table_create(
            cluster,
            table,
            config, 
            **kw
        )
    @server.trace
    async def cluster_table_create(cluster: str, table: str, config: dict, **kw):
        pyql = await server.env['PYQL_UUID']
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        trace = kw['trace']

        # check existence of table 

        tables = await server.data['cluster'].tables['tables'].select(
            'name',
            where={
                'cluster': cluster, 
                'name': table
            }
        )

        if len(tables) > 0:
            return {"message": f"table already exists"}
        
        endpoints = await server.data['cluster'].tables['endpoints'].select(
            '*',
            where={'cluster': cluster}
        )

        ep_requests = {}
        create_requests = []
        for endpoint in endpoints:
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']
            async def cluster_table_create():
                try:
                    return {
                        epuuid: await server.rpc_endpoints[epuuid]['create_table'](
                            config
                        )
                    }
                except Exception as e:
                    return {
                        epuuid: {'error': trace.exception(f"error when creating table")}
                        }

            create_requests.append(
                cluster_table_create()
            )
        create_results = await gather_items(create_requests)

        # add tables to pyql tables

        new_table = {
            'id': f"{cluster}_{table}",
            'name': table,
            'cluster': cluster,
            'config': config,
            'is_paused': False
        }

        await cluster_table_change(pyql, 'tables', 'insert', new_table, **kw)

        # update state table
        update_state_tasks = []
        for endpoint in create_results:
            sync_state = 'loaded' if not 'error' in create_results[endpoint] else 'new'
            state_data = {
                'name': f"{endpoint}_{table}",
                'state': 'loaded',
                'table_name': table,
                'cluster': cluster,
                'uuid': endpoint # used for syncing logs
            }
            update_state_tasks.append(
                cluster_table_change(pyql, 'state', 'insert', state_data, **kw)
            )
        update_state_results = await asyncio.gather(
            *update_state_tasks,
            return_exceptions=True
        )
        trace(f"update_state_results - {update_state_results}")

        return {
            "message": trace(f"cluster {cluster} table {table} created - finished")
            }


    @server.api_route('/cluster/{cluster}/table/{table}/select', methods=['GET','POST'])
    async def cluster_table_select(
        cluster: str, 
        table: str, 
        request: Request, 
        query: Query = None,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_select_auth(cluster, table, data=query, request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_select_auth(cluster, table, data=None, **kw):
        trace = kw['trace']
        request = kw['request']
        try:
            return await table_select(
                cluster, table, 
                data=data, 
                method=request.method, **kw)
        except Exception as e:
            server.http_exception(500, trace.exception("error in cluster table select"))

    @server.api_route('/cluster/{cluster}/tables', methods=['GET'])
    async def cluster_tables_config_api(
        cluster: str, 
        request: Request, 
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_tables_config(cluster, request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_tables_config(cluster, **kw):
        return await tables_config(cluster, **kw)

    @server.trace
    async def tables_config(cluster, **kw):
        tables = await server.data['cluster'].tables['tables'].select(
            'name', where={'cluster': cluster}
        )
        tables_config = {}
        for table in tables:
            config = await cluster_table_config(cluster, table['name'], **kw)
            tables_config.update(config)
        return tables_config 

    @server.api_route('/cluster/{cluster}/table/{table}/copy', methods=['GET'])
    async def cluster_table_copy_api(
        cluster: str, 
        table: str, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_copy_auth(cluster, table,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_copy_auth(cluster, table, **kw):
        return await cluster_table_copy(cluster, table, **kw)

    @server.trace
    async def cluster_table_copy(cluster, table, **kw):
        """
        returns a select('*') of a loaded table & the tables 
        last_txn_time
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']
        copy_only = False if not 'copy_only' in kw else kw['copy_only']


        endpoints = await get_table_endpoints(cluster, table, **kw)
        
        endpoints_info = await get_table_info(cluster, table, endpoints, **kw)
        endpoint_choice = random.choice(
            [e for e in endpoints['loaded']]
        )
        endpoint_info = endpoints_info['endpoints'][f"{endpoint_choice}_{table}"]
        path = endpoint_info['path']
        token = endpoint_info['token']

        #TODO - table_copy  / select should use server.data[] if querying local endpoint
        # i.e check if endpoint_info['uuid'] == server.PYQL_NODE_ID

        if endpoint_info['uuid'] == server.PYQL_NODE_ID:
            if not copy_only:
                table_copy = await server.get_table_copy(
                    endpoint_info['db_name'],
                    table
                )
            else:
                table_copy = await server.actions['select'](
                    endpoint_info['db_name'], 
                    table, 
                    **kw
                )
        else:
            # log clusters do not need last_txn_time via copy
            op = 'copy' if not copy_only else 'select' ## 

            # pull table copy & last_txn_time
            table_copy, rc = await server.probe(
                f"{path}/{op}",
                method='GET',
                token=token,
                session=await server.get_endpoint_sessions(
                    endpoint_info['uuid'],
                    **kw
                )
            )
        if not copy_only:
            return table_copy
        
        trace(f"log cluster - table_copy - {table_copy}")
        table_copy = table_copy['data'] if 'data' in table_copy else table_copy
        
        if cluster == pyql and table in ('state', 'tables', 'jobs'):
            return {
                'table_copy': table_copy, 
                'last_txn_time': time.time()
                }

        # log cluster
        last_txn_time = table_copy[-1]['timestamp'] if len(table_copy) > 0 else time.time()
        return {
            'table_copy': table_copy, 
            'last_txn_time': last_txn_time
            }
    server.cluster_table_copy = cluster_table_copy
                
    @server.api_route('/cluster/{cluster}/table/{table}/config', methods=['GET'])
    async def cluster_table_config_api(
        cluster: str, 
        table: str, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_config_auth(cluster, table,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_config_auth(cluster, table, **kw):
        return await cluster_table_config(cluster, table, **kw)

    @server.trace
    async def cluster_table_config(cluster, table, **kw):
        return await endpoint_probe(cluster, table, method='GET', path=f'config', **kw)
    server.cluster_table_config = cluster_table_config



    @server.api_route('/cluster/{cluster}/table/{table}/update', methods=['POST'])
    async def cluster_table_update_api(
        cluster: str, 
        table: str, 
        request: Request, 
        data: Update,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_update(cluster, table, data=data,  request=await server.process_request(request))
    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_update(cluster, table, data=None, **kw):
        return await table_update(cluster=cluster, table=table, data=data, **kw)

    @server.trace
    async def table_update(cluster=None, table=None, data=None, config=None, **kw):
        trace = kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        if not config == None: # this was invoked by a job
            cluster, table, data = config['cluster'], config['table'], config['data']
        return await cluster_table_change(
            cluster, table,'update', data, **kw)
    server.cluster_table_update = table_update
    server.clusterjobs['table_update'] = table_update
            

    @server.api_route('/cluster/{cluster}/table/{table}/insert', methods=['POST'])
    async def cluster_table_insert_api(
        cluster: str, 
        table: str, 
        data: Insert, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_insert(cluster, table, data,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_insert(cluster, table, data, **kw):
        return await table_insert(cluster, table, data, **kw)
    @server.trace
    async def table_insert(cluster, table, data, **kw):
        return await cluster_table_change(cluster, table, 'insert',  data, **kw)
    server.cluster_table_insert = table_insert

    @server.api_route('/cluster/{cluster}/table/{table}/delete', methods=['POST'])
    async def cluster_table_delete_api(
        cluster: str, 
        table: str, 
        data: Delete, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_delete(cluster, table, data,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_delete(cluster, table, data, **kw):
        return await table_delete(cluster, table, data, **kw)

    @server.trace
    async def table_delete(cluster, table, data, **kw):
        return await cluster_table_change(cluster, table, 'delete', data, **kw)

    @server.api_route('/cluster/{cluster}/table/{table}/{key}')
    async def cluster_table_get_by_key(
        cluster: str, 
        table: str, 
        key: str, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_key(cluster, table, key, request=await server.process_request(request))


    @server.api_route('/cluster/{cluster}/table/{table}/{key}', methods=['POST'])
    async def cluster_table_update_by_key(
        cluster: str, 
        table: str, 
        key: str, 
        request: Request, 
        data: KeyUpdate = None,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_key(cluster, table, key, data=data,  request=await server.process_request(request))


    @server.api_route('/cluster/{cluster}/table/{table}/{key}', methods=['DELETE'])
    async def cluster_table_delete_by_key(
        cluster: str, 
        table: str, 
        key: str, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_key(cluster, table, key, data=data,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('cluster')
    @server.cluster_name_to_uuid
    @server.trace
    async def cluster_table_key(cluster, table, key, **kw):
        trace = kw['trace']
        request = kw['request']
        if request.method == 'GET':
            return await endpoint_probe(cluster, table, path=f'{key}', **kw)
        data = None
        if request.method == 'POST':
            try:
                data = kw['data']
            except Exception as e:
                server.http_exception(400, trace.error("expected json input for request"))
        primary = await server.data['cluster'].tables['tables'].select(
                    'config',
                    where={'cluster': cluster, 'name': table}
                )
        primary = primary[0]['config'][table]['primary_key']
        if request.method == 'POST':
            kw['data'] = {'set': data, 'where': {primary: key}}
            return await table_update(cluster=cluster, table=table, **kw)
        if request.method == 'DELETE':
            return await table_delete(cluster, table, {'where': {primary: key}}, **kw)


    @server.api_route('/cluster/{cluster}/table/{table}/pause/{pause}', methods=['POST'])
    async def cluster_table_pause_api(
        cluster: str, 
        table: str, 
        pause: str, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_table_pause(cluster, table, pause,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_table_pause(cluster, table, pause, **kw):
        return await table_pause(cluster, table, pause, trace=kw['trace'])

    @server.trace
    async def table_pause(cluster, table, pause, **kw):
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        pause = True if pause == 'start' else False
        pause_set = {
            'set': {'is_paused': pause},
            'where': {'id': f"{cluster}_{table}"}
        }

        # overrides table paused checks
        kw['force'] = True

        result = await cluster_table_change(pyql, 'tables', 'update', pause_set, **kw)
        if 'delay_after_pause' in kw:
            await asyncio.sleep(kw['delay_after_pause'])
        trace.warning(f'cluster_table_pause {cluster} {table} pause {pause} result: {result}')
        return result
    server.table_pause = table_pause
        
    @server.trace
    async def table_endpoint(cluster, table, endpoint, config=None, **kw):
        """ Sets state 
        cluster: uuid, table: name, endpoint: uuid, 
        config: {'in_sync': True|False, 'state': 'loaded|new'}
        last_mod_time: float(time.time())
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        set_config = config
        valid_inputs = ['in_sync', 'state']
        for cfg in set_config:
            if not cfg in valid_inputs:
                server.http_exception(
                    400, 
                    trace.error(f"invalid input {cfg}, supported config inputs {valid_inputs}")
                    )
        sync_set = {
            'set': set_config,
            'where': {'cluster': cluster, 'table_name': table, 'uuid': endpoint}
        }
        result = await cluster_table_change(pyql, 'state', 'update', sync_set, **kw)
        trace.warning(f'cluster_table_endpoint_sync {cluster} {table} endpoint {endpoint} result: {result}')
        return result




    if not 'PYQL_CLUSTER_ACTION' in os.environ:
        os.environ['PYQL_CLUSTER_ACTION'] = 'join'


    endpoints = await server.data['cluster'].tables['endpoints'].select('*')

    # Table created only if 'init' is passed into os.environ['PYQL_CLUSTER_ACTION']
    tables = []
    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        table_list = ['clusters', 'endpoints', 'tables', 'state', 'jobs', 'auth', 'data_to_txn_cluster']
        tables = [{table_name: await server.get_table_config('cluster', table_name)} for table_name in table_list]
    
    join_job_type = "node" if os.environ['PYQL_CLUSTER_ACTION'] == 'init' or len(endpoints) == 1 else 'cluster'

    join_cluster_job = {
        "job": f"{os.environ['HOSTNAME']}{os.environ['PYQL_CLUSTER_ACTION']}_cluster",
        "job_type": join_job_type,
        "method": "POST",
        "path": "/cluster/pyql/join",
        "data": {
            "name": os.environ['HOSTNAME'],
            "path": f"{os.environ['PYQL_NODE']}:{os.environ['PYQL_PORT']}",
            "port": os.environ['PYQL_PORT'],
            "uuid": server.PYQL_NODE_ID,
            "token": await server.env['PYQL_LOCAL_SERVICE_TOKEN'],
            "database": {
                'name': "cluster",
                'uuid': server.PYQL_NODE_ID
            },
            "tables": tables,
            "consistency": ['clusters', 'endpoints', 'auth'] # defines whether table modifications are cached before submission & txn time is reviewed
        }
    }

    if 'PYQL_CLUSTER_JOIN_TOKEN' in os.environ and os.environ['PYQL_CLUSTER_ACTION'] == 'join':
        join_cluster_job['join_token'] = os.environ['PYQL_CLUSTER_JOIN_TOKEN']

    if await server.env['SETUP_ID'] == server.setup_id:
        await server.internal_job_add(join_cluster_job)

    if len(endpoints) == 1 or os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        ready_and_quorum = True
        health = 'healthy'
    else:
        await server.data['cluster'].tables['state'].update(
            state='stale', where={'uuid': server.PYQL_NODE_ID}
        )
        ready_and_quorum = False
        health = 'healing'
    # Sets ready false for any node with may be restarting as resync is required before marked ready

    await server.data['cluster'].tables['quorum'].insert(
        **{
            'node': server.PYQL_NODE_ID,
            'nodes': {server.PYQL_NODE_ID: time.time()},
            'missing': {},
            'in_quorum': ready_and_quorum,
            'health': health,
            'last_update_time': float(time.time()),
            'ready': ready_and_quorum,
        }
    )