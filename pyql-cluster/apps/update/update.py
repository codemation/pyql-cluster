# update
def run(server):
    from flask import request
    import os
    @server.route('/db/<database>/table/<table>/update', methods=['POST'])
    def update_func(database, table):
        message, rc = server.check_db_table_exist(database,table)
        if rc == 200:
            table = server.data[database].tables[table]
            params = request.get_json()
            if 'set' not in params or 'where' not in params:
                return f"""missing key-values set: {'{"columnName": "value"}'} and where: {'{"columnName": "value"}'}""", 400
            response = table.update(**params['set'], where=params['where'])
            return {"status": 200, "message": "OK"}
        return message,rc
