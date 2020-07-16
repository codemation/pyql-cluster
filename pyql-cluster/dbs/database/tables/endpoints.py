async def db_attach(server):
    import os
    db = server.data['cluster']
    await db.create_table(
        'endpoints',
        [
            ('uuid', str, 'UNIQUE NOT NULL'),
            ('db_name', str),
            ('path', str),
            ('token', str),
            ('cluster', str)
        ],
        'uuid'
    )
    return # Enter db.create_table statement here
    