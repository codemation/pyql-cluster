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
                db_name = os.getenv('DB_NAME').rstrip()
                db_location = os.environ['PYQL_VOLUME_PATH']
                config['database'] = f'{db_location}/{db_name}'
        else:
            with open('.cmddir', 'r') as projDir:
                for project_path in projDir:
                    db_name = os.getenv('DB_NAME').rstrip()
                    config['database'] = f'{project_path}dbs/database/{db_name}'
        config['logger'] = log
        if server.PYQL_DEBUG == True:
            config['debug'] = True

        from pyql import data
        import sqlite3
        from . import setup
        log.info("finished imports")
        server.data[db_name] = data.Database(sqlite3.connect, **config)
        log.info("finished dbsetup")
        setup.attach_tables(server)
        log.info("finished attach_tables")
        return {"status": 200, "message": "database attached successfully"}, 200
    response = database_attach()
    log.info(response)