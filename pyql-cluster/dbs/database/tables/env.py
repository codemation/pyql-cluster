
async def db_attach(server):
    db = server.data['cluster']
    await db.create_table(
       'env', [
           ('env', str, 'UNIQUE NOT NULL'), 
           ('val', str)
       ],
       'env'
    )
    server.env = db.tables['env']