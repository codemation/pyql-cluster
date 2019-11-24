# insert
def run(server):
    from flask import request
    import os
    @server.route('/insert/<table>', methods=['POST'])
    def insert_func(table):
        table = server.data[os.environ['DB_NAME']].tables[table]
        params = request.get_json()
        for k in params: 
            if not k in table.columns:
                return f"invalid key provided {k} not found in table {table}", 400
            
        response = table.insert(**params)
        return {"status": 200, "message": "items added"}, 200
            