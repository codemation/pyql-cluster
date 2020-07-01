# insert
def run(server):
    from flask import request
    log = server.log
    @server.route('/db/<database>/table/<table>/insert', methods=['POST'])
    @server.is_authenticated('local')
    def insert_func(database,table, params=None):
        message, rc = server.check_db_table_exist(database,table)
        if rc == 200:
            table = server.data[database].tables[table]
            params = request.get_json() if params == None else params
            data = {}
            for k,v in params.items(): 
                if not k in table.columns:
                    error = f"invalid key provided '{k}' not found in table {table.name}, valid keys {[col for col in table.columns]}"
                    log.error(error)
                    return {"error": error}, 400
            try:
                response = table.insert(**params)
            except Exception as e:
                return {"error": log.exception(f"error inserting into {database} {table} using {params} - {repr(e)}")}, 400
            return {"message": "items added"}, 200
        else:
            return message, rc
    server.actions['insert'] = insert_func