async def run(server):
    import os, uuid
    import asyncio
    from datetime import datetime
    from fastapi import Request, Depends


    log = server.log

    @server.trace   
    async def pyql_join_txn_cluster(config: dict, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']

        # check if can join any current txn cluster 
        # or create new cluster - limit 3 pyql endpoints
        # per txn cluster
        txn_clusters = await server.data['cluster'].tables['clusters'].select(
            'clusters.id',
            'clusters.name',
            'endpoints.uuid',
            'endpoints.token',
            'endpoints.path',
            join={
                'endpoints': {
                    'clusters.id': 'endpoints.cluster'
                }
            },
            where={
                'clusters.type': 'log'
            }
        )
        log_clusters_and_endpoints = {}
        for cluster in txn_clusters:
            if not cluster['clusters.id'] in log_clusters_and_endpoints:
                log_clusters_and_endpoints[cluster['clusters.id']] = []
            log_clusters_and_endpoints[cluster['clusters.id']].append(
                cluster['endpoints.uuid']
            )
        joined_existing = False
        for cluster in log_clusters_and_endpoints:
            if len(log_clusters_and_endpoints[cluster]) < 3:
                # this node can join this cluster
                data = {
                    'id': str(uuid.uuid1()),
                    'uuid': config['database']['uuid'],
                    'db_name': 'transactions',
                    'path': config['path'],
                    'token': config['token'],
                    'cluster': cluster
                }
                await server.server.cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)

                # need to create state entries for new endpoints
                # for each table in log_cluster - create state entry for new endpoint
                tables = await server.data['cluster'].tables['tables'].select('name', where={'cluster': cluster})
                tables = [table['name'] for table in tables]
                for table in tables:
                    data = {
                        'name': f"{config['database']['uuid']}_{table}",
                        'state': 'new',
                        'table_name': table,
                        'cluster': cluster,
                        'uuid': config['database']['uuid'], # used for syncing logs
                        'last_mod_time': 0.0
                    }
                    await server.server.cluster_table_change(pyql, 'state', 'insert', data, **kw)



                joined_existing = True
        if not joined_existing:
            await server.pyql_create_txn_cluster(
                config,
                txn_clusters,
                **kw
            )
    @server.trace
    async def join_cluster_create(cluster_name, config, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        # Creating a New Cluster
        cluster_id = str(uuid.uuid1())
        trace(f"creating new cluster with id {cluster_id}")

        data = {
            'id': cluster_id,
            'name': cluster_name,
            'owner': kw['authorization'], # added via @server.is_autenticated
            'access': {'allow': [kw['authorization']]},
            'type': 'data',
            'created_by_endpoint': config['name'],
            'create_date': f'{datetime.now().date()}'
            }
        create_result = await server.server.cluster_table_change(pyql, 'clusters', 'insert', data, **kw)
        trace(f"create cluster result: {create_result}")

        # Adding new Cluster to data_to_txn_cluster
        # ('data_cluster_id', str, 'UNIQUE'),
        # ('txn_cluster_id', str)
        txn_cluster = await server.get_txn_cluster_to_join(trace=trace)
        trace(f"txn cluster to join: {txn_cluster}")

        new_cluster_to_txn_map = {
            'data_cluster_id': cluster_id,
            'txn_cluster_name': txn_cluster['name'],
            'txn_cluster_id': txn_cluster['id'] # await get_txn_cluster_to_join()
        }
        join_txn_cluster_result = await server.server.cluster_table_change(
            pyql, 
            'data_to_txn_cluster', 
            'insert', 
            new_cluster_to_txn_map, 
            **kw
        )
        trace(f"join txn cluster - result: {join_txn_cluster_result}")
        return trace(f"completed")

    @server.trace
    async def join_cluster_create_or_update_endpoint(cluster_id, config, **kw):
        """
        called by join_cluster to create new cluster endpoint
        or update the config of an existing 
        """
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        trace(f"started for cluster: {cluster_id}")

        endpoints = await server.data['cluster'].tables['endpoints'].select(
            'uuid', 
            where={'cluster': cluster_id}
        )
        new_endpoint_or_database = False
        if not config['database']['uuid'] in [ endpoint['uuid'] for endpoint in endpoints ]:
            #add endpoint
            new_endpoint_or_database = True
            new_endpoint_id = str(uuid.uuid1())
            data = {
                'id': str(uuid.uuid1()),
                'uuid': config['database']['uuid'],
                'db_name': config['database']['name'],
                'path': config['path'],
                'token': config['token'],
                'cluster': cluster_id
            }

            trace(f"adding new endpoint with id: {new_endpoint_id} {config['database']}")

            await server.server.cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)

        else:
            #update endpoint latest path info - if different
            trace(
                f"endpoint with id {config['database']['uuid']} already exists in cluster {cluster_id} endpoints {endpoints}"
                )
            update_set = {
                'set': {
                    'path': config['path'], 
                    'token': config['token']},
                'where': {
                    'uuid': config['database']['uuid']
                }
            }
            if len(endpoints) == 1 and cluster_id == pyql:
                #Single node pyql cluster - path changed
                await server.data['cluster'].tables['endpoints'].update(
                    **update_set['set'],
                    where=update_set['where']
                )
            else:
                await server.server.cluster_table_change(pyql, 'endpoints', 'update', update_set, **kw)
                if cluster_id == await server.env['PYQL_UUID']:
                    trace(f"existing endpoint detected for multi-node cluster, marking new endpoint tables 'stale'")
                    await server.server.cluster_table_change(
                        pyql, 'state', 'update', 
                        {
                            'set': {
                                'state': 'stale',
                                'info': {
                                    'stale reason': 'existing endpoint rejoined cluster',
                                    'operation': trace.get_root_operation(),
                                    'node': server.PYQL_NODE_ID
                                }
                            }, 
                            'where': {
                                'uuid': config['database']['uuid']
                                }
                        }, 
                        **kw
                    )
        trace.warning(f"completed - new_endpoint_or_database: {new_endpoint_or_database}")
        return new_endpoint_or_database

    @server.trace
    async def join_cluster_create_tables(cluster_id: str, config: dict, **kw) -> list:
        """
        creates new tables if not created already and returns
        list of created tables
        """
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        trace(f"started for cluster: {cluster_id}")

        tables = await server.data['cluster'].tables['tables'].select('name', where={'cluster': cluster_id})
        tables = [table['name'] for table in tables]

        txn_cluster_id = await server.data['cluster'].tables['data_to_txn_cluster'].select(
            'txn_cluster_id',
            where={
                'data_cluster_id': cluster_id
            }
        )
        txn_cluster_id = txn_cluster_id[0]['txn_cluster_id']

        # if tables not exist, add
        new_tables = []
        for table in config['tables']:
            for table_name, tb_config in table.items():
                if not table_name in tables:
                    new_tables.append(table_name)
                    #JobIfy - create as job so config
                    data = {
                        'id': f"{cluster_id}_{table_name}",
                        'name': table_name,
                        'cluster': cluster_id,
                        'config': tb_config,
                        'consistency': table_name in config['consistency'],
                        'is_paused': False
                    }
                    await server.server.cluster_table_change(pyql, 'tables', 'insert', data, **kw)
                    if not (cluster_id == pyql and table_name in ['jobs', 'state', 'tables']):
                        cluster_id_under = '_'.join(cluster_id.split('-'))
                        await server.cluster_txn_table_create(
                            txn_cluster_id,
                            f'{cluster_id_under}_{table_name}',
                            **kw
                        )
        trace(f"finished with {new_tables} new tables in cluster")
        return new_tables

    @server.trace
    async def join_cluster_update_state(cluster_id, new_tables, config, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        trace(f"started for cluster: {cluster_id} and new_tables: {new_tables}")

        tables = await server.data['cluster'].tables['tables'].select('name', where={'cluster': cluster_id})
        tables = [table['name'] for table in tables]
        endpoints = await server.data['cluster'].tables['endpoints'].select('*', where={'cluster': cluster_id})    
        state = await server.data['cluster'].tables['state'].select('name', where={'cluster': cluster_id})
        state = [tbEp['name'] for tbEp in state]

        for table in tables:
            for endpoint in endpoints:
                table_endpoint = f"{endpoint['uuid']}_{table}"
                if not table_endpoint in state:
                    # check if this table was added along with endpoint, and does not need to be created 
                    load_state = 'loaded' if endpoint['uuid'] == config['database']['uuid'] else 'new'
                    if not table in new_tables:
                        #Table arleady existed in cluster, but not in endpoint with same table was added
                        sync_state = False
                        load_state = 'new'
                    else:
                        # New tables in a cluster are automatically marked in sync by endpoint which added
                        sync_state = True
                    # Get DB Name to update
                    data = {
                        'name': table_endpoint,
                        'state': load_state,
                        'table_name': table,
                        'cluster': cluster_id,
                        'uuid': endpoint['uuid'], # used for syncing logs
                        'last_mod_time': 0.0
                    }
                    trace(f"adding new table endpoint: {data}")
                    await server.server.cluster_table_change(pyql, 'state', 'insert', data, **kw)
        trace(f"finished")
    @server.trace
    async def join_cluster_pyql_finish_setup(
        cluster_id: str, 
        is_pyql_bootstrapped: bool,
        is_new_endpoint: bool,
        config: dict, 
        **kw):
        """
        used to perform final setup of a new or rejoining pyql node 
        """
        trace = kw['trace']

        trace(f"starting with is_pyql_bootstrapped={is_pyql_bootstrapped}, is_new_endpoint={is_new_endpoint}")

        if is_pyql_bootstrapped and not is_new_endpoint:
            trace(f"Not bootrapping cluster, not a new endpoint, starting tablesync_mgr")
            await tablesync_mgr(trace=trace)

        host, port = config['path'].split(':')
        # setup rpc connection to node
        await server.cluster_add_rpc_endpoint(
            host, 
            port,
            '/ws/pyql-cluster',
            secret=config['token'],
            namespace=config['uuid']
        )

        # using rpc connection - complete setup

        # set PYQL_UUID
        await server.rpc_endpoints[config['uuid']]['set_pyql_id'](
            {'PYQL_UUID': cluster_id }
        )

        # complete auth setup
        result = await server.rpc_endpoints[config['uuid']]['set_service_token'](
            {
                'PYQL_CLUSTER_SERVICE_TOKEN': await server.env['PYQL_CLUSTER_SERVICE_TOKEN']
            },
            server.PYQL_NODE_ID
        )
        
        trace.warning(f"completed auth setup for new pyql endpoint: result {result}")
        # Trigger quorum update
        await cluster_quorum_check(trace=trace)
        return trace("join_cluster_pyql_finish_setup - completed")
    @server.api_route('/cluster/{cluster_name}/join', methods=['POST'])
    async def join_cluster_api(
        cluster_name: str, 
        config: dict, 
        request: Request,
        token: dict = Depends(server.verify_token),
    ):
        return await join_cluster_auth(cluster_name, config,  request=await server.process_request(request))

    @server.is_authenticated('cluster')
    @server.trace
    async def join_cluster_auth(cluster_name, config, **kw):
        return await join_cluster(cluster_name, config, **kw)

    @server.trace
    async def join_cluster(cluster_name, config, **kw):
        trace=kw['trace']
        kw['loop'] = asyncio.get_running_loop()
        request = kw['request'] if 'request' in kw else None
    
        db = server.data['cluster']
        new_endpoint_or_database = False
        is_pyql_bootstrapped = False
        pyql = None

        trace.info(f"join cluster for {cluster_name} with kwargs {kw}")

        # check if pyql is bootstrapped 
        clusters = await server.data['cluster'].tables['clusters'].select(
                '*', where={'name': 'pyql'})
        for cluster in clusters:
            if cluster['name'] == 'pyql':
                is_pyql_bootstrapped, pyql = True, cluster['id']

        if is_pyql_bootstrapped and os.environ['PYQL_CLUSTER_ACTION'] == 'init':
            if config['database']['uuid'] == server.PYQL_NODE_ID:
                return {"message": trace("pyql cluster already initialized")}

        if not 'authorization' in kw:
            admin_id = await server.data['cluster'].tables['auth'].select('id', where={'username': 'admin'})
            kw['authorization'] = admin_id[0]['id']

        if not is_pyql_bootstrapped and cluster_name == 'pyql':
            await server.join_cluster_pyql_bootstrap(
                config, **kw
            )
            service_id = await server.data['cluster'].tables['auth'].select(
                'id', 
                where={'parent': kw['authorization']})
            service_id = service_id[0]['id']
            kw['authorization'] = service_id

        else: 
            """Pyql Cluster was already Bootstrapped"""
            if cluster_name == 'pyql':
                await pyql_join_txn_cluster(config, **kw)
                await asyncio.sleep(3)
 
        clusters = await server.data['cluster'].tables['clusters'].select(
            '*', where={'owner': kw['authorization']})

        for cluster in clusters:
            if cluster['name'] == 'pyql':
                await server.env.set_item('PYQL_UUID', cluster['id'])

        if not cluster_name in [cluster['name'] for cluster in clusters]:
            await join_cluster_create(cluster_name, config, **kw)
            await asyncio.sleep(3)
            
        cluster_id = await server.data['cluster'].tables['clusters'].select(
            '*', where={
                    'owner': kw['authorization'], 
                    'name': cluster_name
                })
        cluster_id = cluster_id[0]['id']

        #check for existing endpoint in cluster: cluster_id 
        new_endpoint_or_database = await join_cluster_create_or_update_endpoint(
            cluster_id, config, **kw
        )
        await asyncio.sleep(10)

        # check for exiting tables in cluster 
        new_tables = await join_cluster_create_tables(cluster_id, config, **kw)
        await asyncio.sleep(10)

        if new_endpoint_or_database == True:
            trace(f"new endpoint or database detected, need to join_cluster_update_state")
            await join_cluster_update_state(cluster_id, new_tables, config, **kw)
            await asyncio.sleep(10)

        if cluster_name == 'pyql':
            await join_cluster_pyql_finish_setup(
                cluster_id,
                is_pyql_bootstrapped,
                new_endpoint_or_database,
                config,
                **kw,
            )
        return {"message": trace.warning(f"join cluster {cluster_name} for endpoint {config['name']} completed successfully")}, 200
    server.join_cluster = join_cluster