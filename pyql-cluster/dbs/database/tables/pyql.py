
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
            'pyql', [
                ('uuid', str, 'UNIQUE'), 
                ('database', str), 
                ('lastModTime', float)
                ],
            'uuid'
            )
            db.tables['pyql'].insert(**{
                'uuid': uuid.uuid1(),
                'database': database,
                'lastModTime': time.time()
                })
    pass # Enter db.create_table statement here
            