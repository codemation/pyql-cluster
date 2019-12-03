# insert
def run(server):
    from flask import request
    import os
    @server.route('/db/<database>/table/<table>/insert', methods=['POST'])
    def insert_func(database,table):
        message, rc = server.check_db_table_exist(database,table)
        print(message, rc)
        if rc == 200:
            table = server.data[database].tables[table]
            params = request.get_json()
            for k in params: 
                if not k in table.columns:
                    print(f"invalid key provided {k} not found in table {table} 400")
                    return f"invalid key provided {k} not found in table {table}", 400
                
            response = table.insert(**params)
            return {"status": 200, "message": "items added"}, 200
        else:
            return message, rc
            