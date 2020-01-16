# update
def run(server):
    from flask import request
    @server.route('/db/<database>/table/<table>/update', methods=['POST'])
    def update_func(database, table, params=None):
        message, rc = server.check_db_table_exist(database,table)
        if not rc == 200:
            return {"message": message},rc
        #200
        table = server.data[database].tables[table]
        params = request.get_json() if params == None else params
        if not 'set' in params or not 'where' in params:
            return f"""missing key-values set: {'{"columnName": "value"}'} and where: {'{"columnName": "value"}'}""", 400
        response = table.update(**params['set'], where=params['where'])
        return {"status": 200, "message": "OK"}, 200
        
    server.actions['update'] = update_func