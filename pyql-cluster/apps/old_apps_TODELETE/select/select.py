# select
def run(server):
    from flask import request
    import os
    @server.route('/select/<table>', methods=['GET', 'POST'])
    def select_func(table):
        if request.method == 'GET':
            response = server.data[os.environ['DB_NAME']].tables[table].select('*')
            return {"status": 200, "data": response}, 200
        else:
            params = request.get_json()
            print(params)
            if 'select' in params:
                response = server.data[os.environ['DB_NAME']].tables[table].select(
                    *params['select'], 
                    where=params['where'] if 'where' in params else {}
                    )
                return {"status": 200, "data": response}, 200
            else:
                return "missing selection", 400