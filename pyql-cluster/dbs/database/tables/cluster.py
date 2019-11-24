
def db_attach(server):
    import os
    db = server.data[os.environ['DB_NAME']]
    db.create_table(
        'cluster',
        [
            ('clusterName', str),
            ('nodes', str),
            ('databases', str),
            ('tables', str)
        ],
        'clusterName'
    )
    pass # Enter db.create_table statement here
            