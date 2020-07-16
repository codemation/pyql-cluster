# delete
async def run(server):
    from fastapi import Request

    @server.api_route('/db/{database}/table/{table}/delete', methods=['POST'])
    async def delete_func_api(database: str, table: str, params: dict, request: Request):
        return await delete_func(database, table, params,  request=await server.process_request(request))
    @server.is_authenticated('local')
    async def delete_func(database, table, params, **kw):
        return await delete(database, table, params, **kw)
    async def delete(database, table, params, **kw):
        message, rc = server.check_db_table_exist(database, table)
        if rc == 200:
            table = server.data[database].tables[table]
            response = await table.delete(where=params['where'])
            return {"message": "OK"}
        else:
            server.http_exception(rc, message)
    server.actions['delete'] = delete