async def db_attach(server):
    db = server.data['cluster']
    await db.create_table(
       'internaljobs', [
           ('id', str, 'UNIQUE NOT NULL'),
           ('name', str, 'UNIQUE NOT NULL'),
           ('status', str), 
           ('config', str)
       ],
       'id'
    )