async def db_attach(server):
    import uuid
    db = server.data['cluster']
    if not 'env' in db.tables:
        await db.create_table(
        'env', [
            ['env', 'str', 'UNIQUE NOT NULL'], 
            ['val', 'str']
        ],
        'env',
        cache_enabled=True
        )
    server.env = db.tables['env']

    PYQL_NODE_ID = await db.tables['env']['PYQL_NODE_ID']
    server.log.warning(f"PYQL_NODE_ID: {PYQL_NODE_ID}")

    if PYQL_NODE_ID is None:
        server.PYQL_NODE_ID = str(uuid.uuid1())
        # create PYQL_NODE_ID reference in env table
        await db.tables['env'].set_item('PYQL_NODE_ID', server.PYQL_NODE_ID)
    else:
        server.PYQL_NODE_ID = PYQL_NODE_ID