async def db_attach(server):
    import os
    db = server.data['cluster']
    if 'tables' in db.tables:
        return
    await db.create_table(
       'tables', [
            ('id', str, 'NOT NULL'),
            ('name', str, 'NOT NULL'),
            ('database', str),
            ('cluster', str),
            ('config', str),
            ('consistency', bool),
            ('is_paused', bool)
       ],
        'id',
        cache_enabled=True
    )
    return # Enter db.create_table statement here
            