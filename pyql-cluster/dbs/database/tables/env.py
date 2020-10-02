async def db_attach(server):
    db = server.data['cluster']
    if not 'env' in db.tables:
        await db.create_table(
        'env', [
            ('env', str, 'UNIQUE NOT NULL'), 
            ('val', str)
        ],
        'env',
        cache_enabled=True
        )
    server.env = db.tables['env']