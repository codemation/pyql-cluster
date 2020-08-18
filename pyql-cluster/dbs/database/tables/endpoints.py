async def db_attach(server):
    import os
    db = server.data['cluster']
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
    