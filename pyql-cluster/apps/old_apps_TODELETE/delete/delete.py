# delete
def run(server):
    from flask import request
    import os
    @server.route('/delete/<table>', methods=['POST'])
    def delete_func(table):
        table = server.data[os.environ['DB_NAME']].tables[table]
        params = request.get_json()
        if not 'where' in params:
            return f"""missing key-value pair "where": {'{"column": "value"}'} for delete""", 400
        response = table.delete(where=params['where'])
        return {"status": 200, "message": "OK"}