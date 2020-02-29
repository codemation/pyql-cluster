
def db_attach(server):
    db = server.data['cluster']    
    db.create_table(
       'transactions', 
       [
           ('uuid', str, 'UNIQUE'),
           ('endpoint', str), 
           ('tableName', str), 
           ('cluster', str),
           ('timestamp', float),
           ('txn', str)
       ],
       'uuid'
    )
    pass # Enter db.create_table statement here
            