
async def db_attach(server):
    db = server.data['cluster']
    if 'state' in db.tables:
        return
    await db.create_table(
       'state', [
            ('name', str, 'UNIQUE NOT NULL'),
            ('state', str),
            ('table_name', str),
            ('cluster', str),
            ('uuid', str), # used for syncing logs 
            ('last_mod_time', float),
            ('info', str)
       ],
       'name',
       cache_enabled=True
    )
    return # Enter db.create_table statement here
            