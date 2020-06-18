
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'quorum', [
           ('node', str, 'UNIQUE NOT NULL'), 
           ('ready', bool), 
           ('inQuorum', bool),
           ('health', str),
           ('nodes', str),
           ('missing', str), 
           ('lastUpdateTime', float)
       ],
       'node'
    )
    pass # Enter db.create_table statement here
            