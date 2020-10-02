async def db_attach(server):
    db = server.data['cluster']
    if 'endpoints' in db.tables:
        return
    await db.create_table(
        'endpoints',
        [
            ('id', str, 'UNIQUE NOT NULL'),
            ('uuid', str),
            ('db_name', str),
            ('path', str),
            ('token', str),
            ('cluster', str)
        ],
        'id',
        cache_enabled=True
    )
    return # Enter db.create_table statement here
    