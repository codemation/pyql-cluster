def db_attach(server):
    import os
    db = server.data['cluster']
    """
    db.create_table(
        'endpoints',
        [
            ('name', str, 'UNIQUE NOT NULL'),
            ('path', str),
            ('cluster', str)
        ],
        'name'
    )
    
                        'uuid': config['database']['uuid'],
                    'dbname': config['database']['name'],
                    'path': config['path'],
                    'cluster': clusterName
    """
    db.create_table(
        'endpoints',
        [
            ('uuid', str, 'UNIQUE NOT NULL'),
            ('dbname', str),
            ('path', str),
            ('cluster', str)
        ],
        'uuid'
    )
    pass # Enter db.create_table statement here
    