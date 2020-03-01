def db_attach(server):
    db = server.data['cluster']
    # cache table should reset each time an instance is started / restarted
    def reset_cache():
        server.data['cluster'].run(f'drop table cache')
        db.create_table(
        'cache', [
            ('id', str, 'UNIQUE NOT NULL'), # uuid of cached txn
            ('tableName', str),
            ('type', str), # insert / update / delete / transaction
            ('timestamp', float), # time of txn 
            ('txn', str) # boxy of txn
        ],
        'id'
        )
    server.reset_cache = reset_cache
    server.reset_cache()