# table
def run(server):
    from flask import request
    import os

    @server.route('/tables')
    def get_all_tables_func():
        tables = server.data[os.environ['DB_NAME']].tables
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
    @server.route('/table/<table>')
    def get_table_func(table):
        if table in server.data[os.environ['DB_NAME']].tables:
            table = server.data[os.environ['DB_NAME']].tables[table]
            response = {
                table.name: {
                    "columns": [ {"name": col.name,"type": str(col.type.__name__), "mods": col.mods } for k,col in table.columns.items() ],
                    "primaryKey": table.prim_key
                }        
            }
            return response, 200
        else:
            return f"""table with name {table} does not exist within database {os.environ['DB_NAME']}""", 404
    @server.route('/table/create', methods=['POST'])
    def create_table_func():
        db = server.data[os.environ['DB_NAME']]
        tableConfig = request.get_json()
        convert = {'str': str, 'int': int, 'blob': bytes, 'float': float, 'bool': bool}
        columns = []
        for tableName in tableConfig:
            if not tableName in db.tables:
                if "columns" in tableConfig[tableName]:
                    for col in tableConfig[tableName]["columns"]:
                        if col['type'] in convert:
                            columns.append(
                                (
                                    col['name'],
                                    convert[col['type']],
                                    col['mods']
                                )
                            )
                        else:
                            f"""invalid type {col['type']} provided in column {col['name']}. use: {convert}""", 400
                    colNames = [c[0] for c in columns]
                    if "primaryKey" in tableConfig[tableName]:
                        if tableConfig[tableName]["primaryKey"] in colNames:
                            # All required table configuration has been provided, Creating table.
                            db.create_table(
                                tableName, 
                                columns,
                                tableConfig[tableName]["primaryKey"]
                                )
                            return {"message": f"""table {tableName} created successfully """}, 200
                        else:
                            return f"""provided primaryKey {tableConfig[tableName]["primaryKey"]} is not a column with "columns": {colNames} """
                    else:
                        return f"""missing new table config "primaryKey": <column_name> """,  400
                else:
                    return f"""missing new table config {'"columns": [{"name": "<name>", "type": "<type>", "mods": "<mods>"}, ..]'}""", 400
            else:
                return f"""table {tableName} already exists """, 400

#   