# database - type sqlite3
def run(server):
    import sys, os
    @server.route('/internal/db/attach')
    def database_attach():
        config=dict()
        os.environ['DB_NAME'] = 'cluster'
        with open('.cmddir', 'r') as projDir:
            for projectPath in projDir:
                dbName = os.getenv('DB_NAME').rstrip()
                print(dbName)
                config['database'] = f'{projectPath}dbs/database/{dbName}'
        #USE ENV PATH for PYQL library or /pyql/
        #sys.path.append('/pyql/' if os.getenv('PYQL_PATH') == None else os.getenv('PYQL_PATH'))
        #try:
        from pyql import data
        import sqlite3
        from . import setup
        print("finished imports")
        server.data[dbName] = data.database(sqlite3.connect, **config)
        print("finished dbsetup")
        setup.attach_tables(server)
        print("finished attach_tables")
        return {"status": 200, "message": "database attached successfully"}, 200
        #except Exception as e:
        #    return {"status": 200, "message": repr(e)}, 500
    response = database_attach()
    print(response)