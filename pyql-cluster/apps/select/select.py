# select
def run(server):
    from flask import request
    log = server.log
    @server.route('/db/<database>/table/<table>/select', methods=['GET', 'POST'])
    @server.is_authenticated('local')
    def cluster_select_func(database, table):
        return select_func(database, table, method=request.method)
    def select_func(database,table, params=None, method='GET'):
        message, rc = server.check_db_table_exist(database,table)
        if not rc == 200:
            return {"error": log.error(f"received non 200 rec with message {message}, rc {rc} during check_db_table_exist")}, 500
        if method == 'GET' and params == None:
            response = server.data[database].tables[table].select('*')
        else:
            params = request.get_json() if params == None else params
            if not 'select' in params:
                warning = f"table {table} select - missing selection"
                return {"warning": log.warning(warning)}, 400
            p = {}
            select = params['select']
            if 'join' in params:
                p['join'] = params['join']
            if 'where' in params:
                p['where'] = params['where']
            response = server.data[database].tables[table].select(*select, **p)
        if response == None or isinstance(response, str):
            return {"error": log.error(f"response object returned {response}")}, 500
        response = [response] if isinstance(response, dict) else response
        return {"data": response}, 200

            
    server.actions['select'] = select_func