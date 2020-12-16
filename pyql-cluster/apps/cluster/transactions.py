async def run(server):

    from functools import reduce
    from datetime import datetime
    from fastapi import Request, Depends

    log = server.log

    @server.trace
    async def get_txn_cluster_to_join(**kw):
        """
        Requirements:
        - limit of 3 pyql nodes per 1 txn cluster
        - when limit is reached, create new txn cluster if expanding
        """
        trace = kw['trace']
        trace(f"starting")

        txn_clusters = await server.data['cluster'].tables['clusters'].select(
            '*',
            where={'type': 'log'}
        )
        data_and_txn_clusters = await server.data['cluster'].tables['data_to_txn_cluster'].select('*')
        # need to iterate through server.data['cluster'].tables['clusters'] type=log tables
        # and build data_and_txn_clusters_count based on this when deciding txn
        # cluster to join

        data_and_txn_clusters_count = Counter()
        for cluster_map in data_and_txn_clusters:
            txn_cluster_id = cluster_map['txn_cluster_id']
            if not txn_cluster_id in data_and_txn_clusters_count:
                data_and_txn_clusters_count[txn_cluster_id] = 0
            data_and_txn_clusters_count[txn_cluster_id] +=1
        # add all txn clusterss with 0 members
        for txn_cluster in txn_clusters:
            if not txn_cluster['id'] in data_and_txn_clusters_count:
                data_and_txn_clusters_count[txn_cluster['id']] = 0
        
        trace(f'data_and_txn_clusters_count: - {data_and_txn_clusters_count}')

        # get txn cluster id with least members
        cluster_id = reduce(
            lambda x,y: x if x[1] < y[1] else y, 
            data_and_txn_clusters_count.items()
        )[0]

        for txn_cluster in txn_clusters:
            if txn_cluster['id'] == cluster_id:
                return txn_cluster

    @server.trace
    async def pyql_create_txn_cluster(config, txn_clusters, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']

        # need to create a new txn cluster
        admin_id = await server.data['cluster'].tables['auth'].select('id', where={'username': 'admin'})
        service_id = kw['authorization']
        new_txn_cluster = str(uuid.uuid1())
        data = {
            'id': new_txn_cluster,
            'name': f"{new_txn_cluster}_log",
            'owner': service_id,
            'access': {'allow': [admin_id[0]['id'], service_id]},
            'type': 'log',
            'key': None,
            'created_by_endpoint': config['name'],
            'create_date': f'{datetime.now().date()}'
            }
        await cluster_table_change(pyql, 'clusters', 'insert', data, **kw)

        # add new endpoint to new cluster

        data = {
            'id': str(uuid.uuid1()),
            'uuid': config['database']['uuid'],
            'db_name': 'transactions',
            'path': config['path'],
            'token': config['token'],
            'cluster': new_txn_cluster
        }
        await cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)

        existing_to_be_added = {'ids': set(), 'existing': []}

        # add two unique and random existing endpoints to new cluster
        while len(existing_to_be_added['ids']) < 2:
            existing = random.choice(txn_clusters)
            if not existing['endpoints.uuid'] in existing_to_be_added['ids']:
                existing_to_be_added['ids'].add(existing['endpoints.uuid'])
                existing_to_be_added['existing'].append(existing)
        for existing in existing_to_be_added['existing']:
            data = {
                'id': str(uuid.uuid1()),
                'uuid': existing['endpoints.uuid'],
                'db_name': 'transactions',
                'path': existing['endpoints.path'],
                'token': existing['endpoints.token'],
                'cluster': new_txn_cluster
            }
            await cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)
    server.pyql_create_txn_cluster = pyql_create_txn_cluster

    @server.trace
    async def cluster_txn_table_create(txn_cluster_id: str, data_cluster_and_table: str, **kw):
        """
        txn table name is composed of the following
        txn_{data_cluster_id}_{table_name}
        """
        txn_cluster_id_underscored = (
            '_'.join(txn_cluster_id.split('-'))
        )
        config = {
            f'txn_{data_cluster_and_table}': {
                "columns": [
                    {
                        "name": "timestamp",
                        "type": "float",
                        "mods": "UNIQUE"
                    },
                    {
                        "name": "txn",
                        "type": "str",
                        "mods": ""
                    }
                ],
                "primary_key": "timestamp", # "foreign_keys": None,
                "cache_enabled": True
            }
        }

        return await server.cluster_table_create(
            txn_cluster_id,
            f"txn_{data_cluster_and_table}",
            config,
            **kw
        )