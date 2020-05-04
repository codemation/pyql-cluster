# table
def run(server):
    from flask import request
    log = server.log

    @server.route('/db/<database>/tables')
    @server.is_authenticated('local')
    def get_all_tables_func(database):
        if not database in server.data:
            return {"message": f"no database with name {database} attached to endpoint"}, 400
        tables = server.data[database].tables
        tablesConfig = []
        for table in tables:
            tablesConfig.append(
                {
                    table: {
                        "primaryKey": tables[table].prim_key,
                        "foreignKeys": tables[table].fKeys,
                        "columns": [{"name": col.name,"type": str(col.type.__name__), 
                        "mods": col.mods } for k,col in tables[table].columns.items() ]
                    }
                }
            ) 
        return {"tables": tablesConfig}, 200

    @server.route('/db/<database>/table/<table>', methods=['GET', 'PUT', 'POST'])
    @server.is_authenticated('local')
    def db_table(database, table):
        if request.method == 'GET':
            return db_table_get(database, table)
        if request.method == 'PUT' or request.method == 'POST':
            return db_table_put_post(database, table)

    def db_table_get(database, table):
        return server.actions['select'](database, table)
    def db_table_put_post(database, table):
        return server.actions['insert'](database, table)


    @server.route('/db/<database>/table/<table>/<key>', methods=['GET', 'POST', 'DELETE'])
    @server.is_authenticated('local')
    def db_table_key(database, table, key):
        primaryKey = server.data[database].tables[table].prim_key
        if request.method == 'GET':
            return db_table_key_get(database, table, primaryKey, key)
        if request.method == 'POST':
            return db_table_key_post(database, table, primaryKey, key)
        if request.method == 'DELETE':
            return db_table_key_delete(database, table, primaryKey, key)

    def db_table_key_get(database, table, key, val):
        return server.actions['select'](database, table, {'select': ['*'], 'where': {key: val}})
    def db_table_key_post(database, table, key, val):
        try:
            setVals = request.get_json()
        except Exception as e:
            log.exception("db_table_key_post missing json input for setVals")
            setVals = {}
        return server.actions['update'](database, table, {'set': setVals, 'where': {key: val}})
    def db_table_key_delete(database, table, key, val):
        return server.actions['delete'](database, table, {'where': {key: val}})


    @server.route('/db/<database>/table/<table>/config')
    @server.is_authenticated('local')
    def db_get_table_config(database,table):
        return get_table_config(database, table)
    def get_table_config(database,table):
        message, rc = server.check_db_table_exist(database,table)
        if not rc == 200:
            return message, rc
        table = server.data[database].tables[table]
        response = {
            table.name: {
                "columns": [ {
                    "name": col.name,
                    "type": str(col.type.__name__), 
                    "mods": col.mods } for k,col in table.columns.items()],
                "primaryKey": table.prim_key,
                "foreignKeys": table.fKeys
            }        
        }
        return response, 200
            
    server.get_table_func = get_table_config

    @server.route('/db/<database>/table/<table>/create', methods=['POST'])
    @server.is_authenticated('local')
    def database_table_create(database, table):
        newTableConfig = request.get_json()
        return create_table_func(database, newTableConfig)

    @server.route('/db/<database>/table/<table>/sync', methods=['POST'])
    @server.is_authenticated('local')
    def sync_table_func(database, table):
        if not database in server.data:
            server.db_check(database)
        if not database in server.data:
            return {"message": log.error(f"{database} not found in endpoint")}, 500
        if not table in server.data[database].tables:
            server.db_check(database)
        if not table in server.data[database].tables:
            return {'message': log.error(f"{table} not found in database {database}")}, 400
        dataToSync = request.get_json()
        tableConfig, _ = db_get_table_config(database, table)
        server.data[database].run(f'drop table {table}')
        message, rc = create_table_func(database, tableConfig)
        log.warning(f"table /sync create_table_func response {message} {rc}")
        for row in dataToSync['data']:
            log.warning(f"table sync insert row - {row}")
            server.data[database].tables[table].insert(**row)
        return {"message": log.warning(f"{database} {table} sync successful")}, 200


    @server.route('/db/<database>/table/create', methods=['POST'])
    @server.is_authenticated('local')
    def db_create_table_func(database):
        return create_table_func(database)
    def create_table_func(database, config=None):
        if database in server.data:
            db = server.data[database]
            tableConfig = request.get_json() if config == None else config
            convert = {'str': str, 'int': int, 'blob': bytes, 'float': float, 'bool': bool}
            columns = []
            for tableName in tableConfig:
                if tableName in db.tables:
                    log.warning(f'table {tableName} already exists - trying anyway')
                if not "columns" in tableConfig[tableName]:
                    return {
                        "error": log.error(f"""missing new table config {'"columns": [{"name": "<name>", "type": "<type>", "mods": "<mods>"}, ..]'}""")
                        }, 400
                
                for col in tableConfig[tableName]["columns"]:
                    if not col['type'] in convert:
                        return {
                            "error": log.error(f"""invalid type {col['type']} provided in column {col['name']}. use: {convert}""")
                            }, 400
                    columns.append((col['name'], convert[col['type']], col['mods']))
                colNames = [c[0] for c in columns]
                if not "primaryKey" in tableConfig[tableName]:
                    return {'error': log.error(f'missing new table config "primaryKey": <column_name>')},  400
                if not tableConfig[tableName]["primaryKey"] in colNames:
                    error = f'provided primaryKey {tableConfig[tableName]["primaryKey"]} is not a column with "columns": {colNames}'
                    return {'error': log.error(error)}, 400
                if 'foreignKeys' in tableConfig[tableName]:
                    for localKey, forignKey in tableConfig[tableName]['fKeys'].items():
                        if not localKey in colNames:
                            return {"error": log.error(f"localKey {localKey} is not a valid column")}, 400
                # All required table configuration has been provided, Creating table.
                db.create_table(
                    tableName, 
                    columns,
                    tableConfig[tableName]["primaryKey"],
                    fKeys=tableConfig[tableName]["fKeys"]
                    )
                server.db_check(database)
                return {"message": log.warning(f"""table {tableName} created successfully """)}, 200