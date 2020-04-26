# select
def run(server):
    from flask import request
    log = server.log
    @server.route('/db/<database>/table/<table>/select', methods=['GET', 'POST'])
    @server.is_authenticated('local')
    def cluster_select_func(database, table):
        return select_func(database, table)
    def select_func(database,table, params=None):
        message, rc = server.check_db_table_exist(database,table)
        if rc == 200:
            if request.method == 'GET' and params == None:
                response = server.data[database].tables[table].select('*')
                return {"status": 200, "data": response}, 200
            else:
                params = request.get_json() if params == None else params
                if 'select' in params:
                    p = {}
                    select = params['select']
                    if 'join' in params:
                        p['join'] = params['join']
                    if 'where' in params:
                        p['where'] = params['where']
                    response = server.data[database].tables[table].select(*select, **p)
                    return {"data": response}, 200
                else:
                    warning = f"table {table} select - missing selection"
                    log.warning(warning)
                    return {"warning": warning}, 400
        else: 
            return message, rc
    server.actions['select'] = select_func