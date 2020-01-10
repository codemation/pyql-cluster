
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'state', [
            ('name', str, 'UNIQUE NOT NULL'),
            ('state', str),
            ('inSync', bool),
            ('tableName', str),
            ('cluster', str),
            ('uuid', str), # used for syncing logs 
            ('lastModTime', float)
       ],
       'name'
    )
    pass # Enter db.create_table statement here
            