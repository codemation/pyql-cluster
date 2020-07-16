# insert
async def run(server):
    from fastapi import Request
    log = server.log

    @server.api_route('/db/{database}/table/{table}/insert', methods=['POST'])
    async def insert_func_api(database: str, table: str, params: dict, request: Request):
        return await insert_func(database, table, params,  request=await server.process_request(request))

    @server.is_authenticated('local')
    async def insert_func(database, table, params, **kw):
        message, rc = server.check_db_table_exist(database,table)
        if rc == 200:
            table = server.data[database].tables[table]
            for k,v in params.items(): 
                if not k in table.columns:
                    error = f"invalid key provided '{k}' not found in table {table.name}, valid keys {[col for col in table.columns]}"
                    log.error(error)
                    server.http_exception(400, error)
            try:
                response = await table.insert(**params)
            except Exception as e:
                server.http_exception(
                    400, 
                    log.exception(f"error inserting into {database} {table} using {params} - {repr(e)}")
                    )
            return {"message": "items added"}
        else:
            server.http_exception(rc, message)
    server.actions['insert'] = insert_func