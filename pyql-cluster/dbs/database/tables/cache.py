async def db_attach(server):
    db = server.data['cluster']
    # cache table should reset each time an instance is started / restarted
    async def reset_cache():
        try:
            await server.data['cluster'].run(f'drop table cache')
        except Exception as e:
            pass
        await db.create_table(
        'cache', [
            ('id', str, 'UNIQUE NOT NULL'), # uuid of cached txn
            ('table_name', str),
            ('type', str), # insert / update / delete / transaction
            ('timestamp', float), # time of txn 
            ('txn', str) # boxy of txn
        ],
        'id',
        cache_enabled=True
        )
    server.reset_cache = reset_cache
    await server.reset_cache()