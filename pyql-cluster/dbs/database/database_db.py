# database - type sqlite3
def run(server):
    import sys, os
    log = server.log
    @server.route('/internal/db/attach')
    def database_attach():
        config=dict()
        os.environ['DB_NAME'] = 'cluster' # TODO - Add to env variables config later
        if 'PYQL_TYPE' in os.environ:
            if os.environ['PYQL_TYPE'] == 'K8S' or os.environ['PYQL_TYPE'] == 'DOCKER':
                dbName = os.getenv('DB_NAME').rstrip()
                dbLocation = os.environ['PYQL_VOLUME_PATH']
                config['database'] = f'{dbLocation}/{dbName}'
        else:
            with open('.cmddir', 'r') as projDir:
                for projectPath in projDir:
                    dbName = os.getenv('DB_NAME').rstrip()
                    config['database'] = f'{projectPath}dbs/database/{dbName}'
        config['logger'] = log
        #USE ENV PATH for PYQL library or /pyql/
        #sys.path.append('/pyql/' if os.getenv('PYQL_PATH') == None else os.getenv('PYQL_PATH'))
        #try:
        from pyql import data
        import sqlite3
        from . import setup
        log.info("finished imports")
        server.data[dbName] = data.database(sqlite3.connect, **config)
        log.info("finished dbsetup")
        setup.attach_tables(server)
        log.info("finished attach_tables")
        return {"status": 200, "message": "database attached successfully"}, 200
        #except Exception as e:
        #    return {"status": 200, "message": repr(e)}, 500
    response = database_attach()
    log.info(response)