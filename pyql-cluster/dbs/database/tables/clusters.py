
async def db_attach(server):
    import os
    db = server.data['cluster']
    if 'clusters' in db.tables:
        return
    await db.create_table(
        'clusters',
        [
            ('id', str, 'UNIQUE NOT NULL'),
            ('name', str),
            ('owner', str), # UUID of auth user who created cluster 
            ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            ('type', str), # 'data|log'
            ('key', str),
            ('created_by_endpoint', str),
            ('create_date', str)
        ],
        'id',
        cache_enabled=True
    )
    return # Enter db.create_table statement here
            