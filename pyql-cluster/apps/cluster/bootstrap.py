async def run(server):
    import os, uuid
    from datetime import datetime

    log = server.log

    @server.trace
    async def bootstrap_cluster(cluster_id, name, config, **kw):
        """
            runs if this node is targeted by /cluster/pyql/join and pyql cluster does not yet exist
        """
        trace = kw['trace']
        trace.info(f"bootstrap starting for {config['name']} config: {config}")

        async def get_clusters_data():
            # ('id', str, 'UNIQUE NOT NULL'),
            # ('name', str),
            # ('owner', str), # UUID of auth user who created cluster 
            # ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            # ('created_by_endpoint', str),
            # ('create_date', str)
            admin_id = kw['authorization']
            service_id = server.data['cluster'].tables['auth'].select(
                'id', 
                where={'parent': admin_id})
            service_id = service_id[0]['id']

            return {
                'id': cluster_id,
                'name': name,
                'owner': service_id,
                'access': {'allow': [admin_id, service_id]},
                'type': 'data' if name == 'pyql' else 'log',
                'key': server.encode(
                    os.environ['PYQL_CLUSTER_INIT_ADMIN_PW'],
                    key=await server.env['PYQL_CLUSTER_TOKEN_KEY']
                    ) if name == 'pyql' else None,
                'created_by_endpoint': config['name'],
                'create_date': f'{datetime.now().date()}'
                }
        def get_endpoints_data(cluster_id):
            return {
                'id': str(uuid.uuid1()),
                'uuid': config['database']['uuid'],
                'db_name': config['database']['name'],
                'path': config['path'],
                'token': config['token'],
                'cluster': cluster_id
            }
        def get_databases_data(cluster_id):
            return {
                'name': f'{config["name"]}_{config["database"]["name"]}',
                'cluster': cluster_id,
                'uuid': config['database']['uuid'],
                'db_name': config['database']['name'],
                'endpoint': config['name']
            }
        def get_tables_data(table, cluster_id, cfg, consistency):
            return {
                'id': f"{cluster_id}_{table}",
                'name': table,
                'cluster': cluster_id,
                'config': cfg,
                'consistency': consistency,
                'is_paused': False
            }
        def get_state_data(table, cluster_id):
            return {
                'name': f'{config["database"]["uuid"]}_{table}',
                'state': 'loaded',
                'table_name': table,
                'cluster': cluster_id,
                'uuid': config['database']['uuid'], # used for syncing logs 
                'last_mod_time': time.time()
            }
        async def execute_request(endpoint, db, table, action, data):
            await server.data[db].tables[table].insert(**data)
        localhost = f'http://localhost:{os.environ["PYQL_PORT"]}'
        # cluster table
        cluster_data = await get_clusters_data()
        await execute_request(localhost, 'cluster', 'clusters',
            'insert', clusterData)

        await server.data['cluster'].tables['clusters'].insert(
            **cluster_data
        )
        # endpoints
        await server.data['cluster'].tables['endpoints'].insert(
            **get_endpoints_data(cluster_data['id'])
        )

        # tables & state 
        for table in config['tables']:
            print(table)
            for table_name, cfg in table.items():
                await server.data['cluster'].tables['tables'].insert(
                    **get_tables_data(
                        table_name,
                        cluster_data['id'], 
                        cfg,
                        table_name in config['consistency']
                    )
                )
        for table in config['tables']:
            for name, cfg in table.items():
                await server.data['cluster'].tables['state'].insert(
                    **get_state_data(name, clusterData['id'])
                )
        trace.info("finished bootstrap")
    server.bootstrap_cluster = bootstrap_cluster

    @server.trace
    async def join_cluster_pyql_bootstrap(config, **kw):
        trace = kw['trace']

        pyql = str(uuid.uuid1())
        pyql_txns_id = str(uuid.uuid1())

        # Boostrap initial txn log cluster
        txn_cluster_data = {
            "name": os.environ['HOSTNAME'],
            "path": f"{os.environ['PYQL_NODE']}:{os.environ['PYQL_PORT']}",
            "token": await server.env['PYQL_LOCAL_SERVICE_TOKEN'],
            "database": {
                'name': "transactions",
                'uuid': f"{server.PYQL_NODE_ID}"
            },
            "tables": [
                await server.get_table_config('transactions', 'txn_cluster_tables')
            ],
            "consistency": [] 
        }

        # Bootstrap pyql cluster
        await bootstrap_cluster(pyql, 'pyql', config, **kw)


        # For each table in bootstrap, create corresponding txn table
        for table in config['tables']:
            for table_name, _ in table.items():
                if table_name in ['state', 'tables', 'jobs']:
                    continue
                await server.create_txn_cluster_table(pyql, table_name)
                pyql_id_underscored = (
                    '_'.join(pyql.split('-'))
                )
                txn_cluster_data['tables'].append(
                    await server.get_table_config(
                        'transactions', 
                        f'txn_{pyql_id_underscored}_{table_name}'
                    )
                )
        # Bootstrap txn cluster
        await bootstrap_cluster(pyql_txns_id, f"{pyql_txns_id}_log", txn_cluster_data, **kw)
        
        # after bootstrap - assign auth to service id 

        service_id = await server.data['cluster'].tables['auth'].select(
            'id', where={'parent': kw['authorization']})
        kw['authorization'] = service_id[0]['id']

        # Register Bootstrapped node in data_to_txn_cluster

        await server.data['cluster'].tables['data_to_txn_cluster'].insert(
            **{
                'data_cluster_id': pyql,
                'txn_cluster_id': pyql_txns_id,
                'txn_cluster_name': pyql_txns_id
            }
        )
    server.join_cluster_pyql_bootstrap = join_cluster_pyql_bootstrap