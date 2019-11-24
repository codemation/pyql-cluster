# update
def run(server):
    from flask import request
    import os
    @server.route('/update/<table>', methods=['POST'])
    def update_func(table):
        table = server.data[os.environ['DB_NAME']].tables[table]
        params = request.get_json()
        if 'set' not in params or 'where' not in params:
            return f"""missing key-values set: {'{"columnName": "value"}'} and where: {'{"columnName": "value"}'}""", 400
        response = table.update(**params['set'], where=params['where'])
        return {"status": 200, "message": "OK"}