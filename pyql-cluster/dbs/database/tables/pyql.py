def db_attach(server):
    """
        special table added to all dbs in server.data for id / state tracking
    """
    import uuid, time
    for database in server.data:
        print(f"attaching pyql table for db {database}")
        db = server.data[database]
        if not 'pyql' in db.tables:
            db.create_table(
            'pyql', 
            [
                ('uuid', str), 
                ('database', str),
                ('table_name', str),
                ('last_txn_uuid', str), 
                ('last_mod_time', float)
            ],
            'table_name'
            )
            db_uuid = uuid.uuid1()
            for tb in server.data[database].tables:
                db.tables['pyql'].insert(**{
                    'uuid': db_uuid,
                    'database': database,
                    'table_name': tb,
                    'last_mod_time': time.time()
                    })
    pass # Enter db.create_table statement here