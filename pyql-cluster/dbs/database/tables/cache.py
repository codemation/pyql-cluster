
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'cache', [
           ('id', str, 'UNIQUE NOT NULL'), 
           ('type', str), 
           ('txn', str)
       ],
       'id'
    )
            