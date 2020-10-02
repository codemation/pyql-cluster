
async def db_attach(server):
    db = server.data['transactions']    
    log = server.log
    if not 'txn_cluster_tables' in db.tables:
        await db.create_table(
        'txn_cluster_tables', 
        [
            ('name', str, 'UNIQUE'), # <data_cluster_uuid><table_name>
            ('txn_cluster_id', str),
            ('last_tx_timestamp', float)
        ],
        'name',
        cache_enabled=True
        )
    

    async def create_txn_cluster_table(cluster_id, name):
        """
        creates a table which will receive cluster table transactions
        on table changes
        """
        txn_cluster_id_underscored = (
            '_'.join(cluster_id.split('-'))
        )
        result = await db.create_table(
            f'txn_{txn_cluster_id_underscored}_{name}',
            [
                ('timestamp', float, 'UNIQUE'),
                ('txn', str)
            ],
            'timestamp',
            cache_enabled=True
        )
        log.warning(f"create_txn_cluster_table {cluster_id}_{name} result: {result}")
    server.create_txn_cluster_table = create_txn_cluster_table



            