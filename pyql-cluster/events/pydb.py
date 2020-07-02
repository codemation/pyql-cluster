# Used by workers for referencing db environ vars
import os
def get_db():
    config=dict()
    os.environ['DB_NAME'] = 'cluster' # TODO - Add to env variables config later
    if 'PYQL_TYPE' in os.environ:
        if os.environ['PYQL_TYPE'] == 'K8S' or os.environ['PYQL_TYPE'] == 'DOCKER':
            db_name = os.getenv('DB_NAME').rstrip()
            db_location = os.environ['PYQL_VOLUME_PATH']
            config['database'] = f'{db_location}/{db_name}'
    else:
        with open('.cmddir', 'r') as projDir:
            for projectPath in projDir:
                db_name = os.getenv('DB_NAME').rstrip()
                config['database'] = f'{projectPath}dbs/database/{db_name}'
    from pyql import data
    import sqlite3
    db = data.Database(sqlite3.connect, **config)
    return db