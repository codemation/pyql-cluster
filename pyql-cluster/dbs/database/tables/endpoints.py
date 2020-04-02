def db_attach(server):
    import os
    db = server.data['cluster']
    db.create_table(
        'endpoints',
        [
            ('uuid', str, 'UNIQUE NOT NULL'),
            ('dbname', str),
            ('path', str),
            ('token', str),
            ('cluster', str)
        ],
        'uuid'
    )
    pass # Enter db.create_table statement here
    