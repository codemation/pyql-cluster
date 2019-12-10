def db_attach(server):
    import os
    db = server.data['cluster']
    db.create_table(
       'databases', [
            ('name', str, 'NOT NULL'),
            ('cluster', str),
            ('dbname', str),
            ('uuid', str),
            ('endpoint', str)
       ],
        'name'
    )
    pass # Enter db.create_table statement here
            