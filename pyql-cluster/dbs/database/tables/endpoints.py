def db_attach(server):
    import os
    db = server.data['cluster']
    db.create_table(
        'endpoints',
        [
            ('name', str, 'UNIQUE NOT NULL'),
            ('path', str),
            ('cluster', str)
        ],
        'name'
    )
    pass # Enter db.create_table statement here
            