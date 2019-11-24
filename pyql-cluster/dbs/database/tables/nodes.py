
def db_attach(server):
    import os
    db = server.data[os.environ['DB_NAME']]
    db.create_table(
        'nodes',
        [
            ('name', str),
            ('lastUpdateTime', str)
        ],
        'name'
    )
    pass # Enter db.create_table statement here
            