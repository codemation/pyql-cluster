
async def db_attach(server):
    db = server.data['cluster']    
    await db.create_table(
       'transactions', 
       [
           ('uuid', str, 'UNIQUE'),
           ('endpoint', str), 
           ('table_name', str), 
           ('cluster', str),
           ('timestamp', float),
           ('txn', str)
       ],
       'uuid'
    )
    return # Enter db.create_table statement here
            