def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'cache', [
           ('id', str, 'UNIQUE NOT NULL'), # uuid of cached txn
           ('tableName', str),
           ('type', str), # insert / update / delete / transaction
           ('timestamp', float), # time of txn 
           ('txn', str) # boxy of txn
       ],
       'id'
    )