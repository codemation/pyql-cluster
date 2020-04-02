# Used by workers for referencing db environ vars
import os
def get_db():
    config=dict()
    os.environ['DB_NAME'] = 'cluster' # TODO - Add to env variables config later
    if 'PYQL_TYPE' in os.environ:
        if os.environ['PYQL_TYPE'] == 'K8S':
            dbName = os.getenv('DB_NAME').rstrip()
            dbLocation = os.environ['PYQL_VOLUME_PATH']
            config['database'] = f'{dbLocation}/{dbName}'
    else:
        with open('.cmddir', 'r') as projDir:
            for projectPath in projDir:
                dbName = os.getenv('DB_NAME').rstrip()
                config['database'] = f'{projectPath}dbs/database/{dbName}'
    from pyql import data
    import sqlite3
    db = data.database(sqlite3.connect, **config)
    return db