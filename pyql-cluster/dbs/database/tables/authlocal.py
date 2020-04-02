
def db_attach(server):
    db = server.data['cluster']
    # 'id': serviceId,
    # 'username': 'pyql',
    # 'type': 'service'
    db.create_table(
       'authlocal', [
           ('id', str, 'UNIQUE NOT NULL'), 
           ('username', str, 'UNIQUE NOT NULL'), 
           ('type', str),
           ('password', str),
       ],
       'id'
    )
    pass # Enter db.create_table statement here
            