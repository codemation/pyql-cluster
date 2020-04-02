
def db_attach(server):
    import os
    db = server.data['cluster']
    db.create_table(
        'clusters',
        [
            ('id', str, 'UNIQUE NOT NULL'),
            ('name', str),
            ('owner', str), # UUID of auth user who created cluster 
            ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            ('key', str),
            ('createdByEndpoint', str),
            ('createDate', str)
        ],
        'id'
    )
    pass # Enter db.create_table statement here
            