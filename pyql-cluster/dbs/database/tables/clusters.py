
def db_attach(server):
    import os
    db = server.data['cluster']
    db.create_table(
        'clusters',
        [
            ('name', str, 'UNIQUE NOT NULL'), 
            ('createdByEndpoint', str),
            ('createDate', str)
        ],
        'name'
    )
    pass # Enter db.create_table statement here
            