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
        tables_config = []
        for table in tables:
            tables_config.append(
                {
                    table: {
                        "primary_key": tables[table].prim_key,
                        "foreign_keys": tables[table].foreign_keys,
                        "columns": [{"name": col.name,"type": str(col.type.__name__), 
                        "mods": col.mods } for k,col in tables[table].columns.items() ]
                    }
                }
            ) 
        return {"tables": tables_config}, 200

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
        return table_key(database, table, key, method=request.method)
    def table_key(database, table, key, method='GET'):
        primary_key = server.data[database].tables[table].prim_key
        if method == 'GET':
            return db_table_key_get(database, table, primary_key, key)
        if method == 'POST':
            return db_table_key_post(database, table, primary_key, key)
        if method == 'DELETE':
            return db_table_key_delete(database, table, primary_key, key)
    server.actions['select_key'] = table_key

    def db_table_key_get(database, table, key, val):
        return server.actions['select'](database, table, {'select': ['*'], 'where': {key: val}})
    
    def db_table_key_post(database, table, key, val):
        try:
            set_vals = request.get_json()
        except Exception as e:
            log.exception("db_table_key_post missing json input for set_vals")
            set_vals = {}
        return server.actions['update'](database, table, {'set': set_vals, 'where': {key: val}})
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
                "primary_key": table.prim_key,
                "foreign_keys": table.foreign_keys
            }        
        }
        return response, 200
            
    server.get_table_func = get_table_config

    @server.route('/db/<database>/table/<table>/create', methods=['POST'])
    @server.is_authenticated('local')
    def database_table_create(database, table):
        new_table_config = request.get_json()
        return create_table_func(database, new_table_config)

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
        data_to_sync = request.get_json()
        table_config, _ = db_get_table_config(database, table)
        server.data[database].run(f'drop table {table}')
        message, rc = create_table_func(database, table_config)
        log.warning(f"table /sync create_table_func response {message} {rc}")
        for row in data_to_sync['data']:
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
            table_config = request.get_json() if config == None else config
            convert = {'str': str, 'int': int, 'blob': bytes, 'float': float, 'bool': bool}
            columns = []
            for table_name in table_config:
                if table_name in db.tables:
                    log.warning(f'table {table_name} already exists - trying anyway')
                if not "columns" in table_config[table_name]:
                    return {
                        "error": log.error(f"""missing new table config {'"columns": [{"name": "<name>", "type": "<type>", "mods": "<mods>"}, ..]'}""")
                        }, 400
                
                for col in table_config[table_name]["columns"]:
                    if not col['type'] in convert:
                        return {
                            "error": log.error(f"""invalid type {col['type']} provided in column {col['name']}. use: {convert}""")
                            }, 400
                    columns.append((col['name'], convert[col['type']], col['mods']))
                col_names = [c[0] for c in columns]
                if not "primary_key" in table_config[table_name]:
                    return {'error': log.error(f'missing new table config "primary_key": <column_name>')},  400
                if not table_config[table_name]["primary_key"] in col_names:
                    error = f'provided primary_key {table_config[table_name]["primary_key"]} is not a column with "columns": {col_names}'
                    return {'error': log.error(error)}, 400
                if 'foreign_keys' in table_config[table_name] and isinstance(table_config[table_name]['foreign_keys'], dict):
                    for local_key, foreign_key in table_config[table_name]['foreign_keys'].items():
                        if not local_key in col_names:
                            return {"error": log.error(f"local_key {local_key} is not a valid column for foreign_key {foreign_key}")}, 400
                else:
                    table_config[table_name]['foreign_keys'] = None
                # All required table configuration has been provided, Creating table.
                db.create_table(
                    table_name, 
                    columns,
                    table_config[table_name]["primary_key"],
                    foreign_keys=table_config[table_name]["foreign_keys"]
                    )
                server.db_check(database)
                return {"message": log.warning(f"""table {table_name} created successfully """)}, 200