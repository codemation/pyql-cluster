# delete
def run(server):
    from flask import request
    @server.route('/db/<database>/table/<table>/delete', methods=['POST'])
    @server.is_authenticated('local')
    def delete_func(database,table, params=None):
        message, rc = server.check_db_table_exist(database,table)
        if rc == 200:
            table = server.data[database].tables[table]
            params = request.get_json() if params == None else params
            if not 'where' in params:
                return {'error': f"""missing key-value pair "where": {'{"column": "value"}'} for delete"""}, 400
            response = table.delete(where=params['where'])
            return {"message": "OK"}, 200
        else:
            return message,rc
    server.actions['delete'] = delete_func