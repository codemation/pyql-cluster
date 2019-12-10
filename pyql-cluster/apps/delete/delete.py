# delete
def run(server):
    from flask import request
    import os
    @server.route('/db/<database>/table/<table>/delete', methods=['POST'])
    def delete_func(database,table, params=None):
        message, rc = server.check_db_table_exist(database,table)
        if rc == 200:
            table = server.data[database].tables[table]
            params = request.get_json() if params == None else params
            if not 'where' in params:
                return f"""missing key-value pair "where": {'{"column": "value"}'} for delete""", 400
            response = table.delete(where=params['where'])
            return {"status": 200, "message": "OK"}
        else:
            return message,rc
    server.actions['delete'] = delete_func
