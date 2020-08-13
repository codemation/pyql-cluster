
async def db_attach(server):
    db = server.data['cluster']
    await db.create_table(
       'quorum', [
           ('node', str, 'UNIQUE NOT NULL'), 
           ('ready', bool), 
           ('in_quorum', bool),
           ('health', str),
           ('nodes', str),
           ('missing', str), 
           ('last_update_time', float)
       ],
       'node',
       cache_enabled=True
    )
    return # Enter db.create_table statement here
            