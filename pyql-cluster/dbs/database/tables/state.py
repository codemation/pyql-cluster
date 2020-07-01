
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'state', [
            ('name', str, 'UNIQUE NOT NULL'),
            ('state', str),
            ('in_sync', bool),
            ('table_name', str),
            ('cluster', str),
            ('uuid', str), # used for syncing logs 
            ('last_mod_time', float)
       ],
       'name'
    )
    pass # Enter db.create_table statement here
            