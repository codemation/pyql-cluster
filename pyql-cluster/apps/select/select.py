# select
def run(server):
    from flask import request
    import os
    @server.route('/db/<database>/table/<table>/select', methods=['GET', 'POST'])
    def select_func(database,table):
        message, rc = server.check_db_table_exist(database,table)
        if rc == 200:
            if request.method == 'GET':
                response = server.data[database].tables[table].select('*')
                return {"status": 200, "data": response}, 200
            else:
                params = request.get_json()
                print(params)
                if 'select' in params:
                    response = server.data[database].tables[table].select(
                        *params['select'], 
                        where=params['where'] if 'where' in params else {}
                        )
                    return {"status": 200, "data": response}, 200
                else:
                    return "missing selection", 400
        else: 
            return message, rc