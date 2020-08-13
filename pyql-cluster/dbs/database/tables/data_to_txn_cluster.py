
async def db_attach(server):
    db = server.data['cluster']    
    await db.create_table(
       'data_to_txn_cluster', 
       [
           ('data_cluster_id', str, 'UNIQUE'),
           ('txn_cluster_id', str),
           ('txn_cluster_name', str)
       ],
       'data_cluster_id',
       cache_enabled=True
    )
    return # Enter db.create_table statement here
            