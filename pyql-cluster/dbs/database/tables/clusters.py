
async def db_attach(server):
    import os
    db = server.data['cluster']
    await db.create_table(
        'clusters',
        [
            ('id', str, 'UNIQUE NOT NULL'),
            ('name', str),
            ('owner', str), # UUID of auth user who created cluster 
            ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            ('key', str),
            ('created_by_endpoint', str),
            ('create_date', str)
        ],
        'id'
    )
    return # Enter db.create_table statement here
            