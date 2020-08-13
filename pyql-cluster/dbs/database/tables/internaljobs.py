async def db_attach(server):
    db = server.data['cluster']
    if 'internaljobs' in db.tables:
        await db.run('drop table internaljobs')
    await db.create_table(
       'internaljobs', [
           ('id', str, 'UNIQUE NOT NULL'),
           ('name', str, 'UNIQUE NOT NULL'),
           ('status', str), 
           ('config', str)
       ],
       'id',
       cache_enabled=True
    )