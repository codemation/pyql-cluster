# table
def run(server):
    from flask import request
    log = server.log

    @server.route('/db/<database>/tables')
    def get_all_tables_func(database):
        if not database in server.data:
            return {"message": f"no database with name {database} attached to endpoint"}, 400
        tables = server.data[database].tables
        tablesConfig = []
        for table in tables:
            tablesConfig.append(
                {
                    table: {
                    "primaryKey": tables[table].prim_key, #"schema": table.get_schema(), Add later
                    "columns": [ {"name": col.name,"type": str(col.type.__name__), "mods": col.mods } for k,col in tables[table].columns.items() ]
                    }
                }
            ) 
        return {"tables": tablesConfig}, 200

    @server.route('/db/<database>/table/<table>')
    def get_table_func(database,table):
        message, rc = server.check_db_table_exist(database,table)
        if not rc == 200:
            return message, rc
        table = server.data[database].tables[table]
        response = {
            table.name: {
                "columns": [ {
                    "name": col.name,
                    "type": str(col.type.__name__), 
                    "mods": col.mods } for k,col in table.columns.items() ],
                "primaryKey": table.prim_key
            }        
        }
        return response, 200
            
    server.get_table_func = get_table_func

    @server.route('/db/<database>/table/<table>/sync', methods=['POST'])
    def sync_table_func(database, table):
        if not database in server.data or not table in server.data[database].tables:
            message = f"{database} or {table} not found in memory"
            log.error(messages)
            return {'message': message}, 400
        dataToSync = request.get_json()
        tableConfig, _ = get_table_func(database, table)
        server.data[database].run(f'drop table {table}')
        message, rc = create_table_func(database, tableConfig)
        log.warning(f"table /sync create_table_func response {message} {rc}")
        for row in dataToSync['data']:
            log.warning(f"table sync insert row - {row}")
            server.data[database].tables[table].insert(**row)
        return {"message": f"{database} {table} sync successful"}, 200

    @server.route('/db/<database>/table/<table>/create', methods=['POST'])
    def database_table_create(database, table):
        newTableConfig = request.get_json()
        return create_table_func(database, newTableConfig)
        
    @server.route('/db/<database>/table/create', methods=['POST'])
    def create_table_func(database, config=None):
        if database in server.data:
            db = server.data[database]
            tableConfig = request.get_json() if config == None else config
            convert = {'str': str, 'int': int, 'blob': bytes, 'float': float, 'bool': bool}
            columns = []
            for tableName in tableConfig:
                if tableName in db.tables:
                    log.warning(f"""table {tableName} already exists - trying anyway""")
                if not "columns" in tableConfig[tableName]:
                    return f"""missing new table config {'"columns": [{"name": "<name>", "type": "<type>", "mods": "<mods>"}, ..]'}""", 400
                
                for col in tableConfig[tableName]["columns"]:
                    if not col['type'] in convert:
                        return f"""invalid type {col['type']} provided in column {col['name']}. use: {convert}""", 400
                    columns.append(
                        (
                            col['name'],
                            convert[col['type']],
                            col['mods']
                        )
                    )
                colNames = [c[0] for c in columns]
                if not "primaryKey" in tableConfig[tableName]:
                    return f"""missing new table config "primaryKey": <column_name> """,  400
                if not tableConfig[tableName]["primaryKey"] in colNames:
                    return f"""provided primaryKey {tableConfig[tableName]["primaryKey"]} is not a column with "columns": {colNames} """, 400
                # All required table configuration has been provided, Creating table.
                db.create_table(
                    tableName, 
                    columns,
                    tableConfig[tableName]["primaryKey"]
                    )
                return {"message": f"""table {tableName} created successfully """}, 200