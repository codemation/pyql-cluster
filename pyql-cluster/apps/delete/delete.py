# delete
async def run(server):
    from fastapi import Request, Depends
    from pydantic import BaseModel

    class Delete(BaseModel):
        where: dict = {'col': 'val'}

    @server.api_route('/db/{database}/table/{table}/delete', methods=['POST'])
    async def delete_func_api(
        database: str, 
        table: str, 
        params: Delete, 
        request: Request, 
        token: dict = Depends(server.verify_token)
    ):
        return await delete_func(database, table, params,  request=await server.process_request(request))
    @server.is_authenticated('local')
    async def delete_func(database, table, params, **kw):
        return await delete(database, table, params, **kw)
    
    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def delete(database, table, params, **kw):
        message, rc = await server.check_db_table_exist(database, table)
        if rc == 200:
            table = server.data[database].tables[table]
            response = await table.delete(where=params['where'])
            return {"message": "OK"}
        else:
            server.http_exception(rc, message)
    server.actions['delete'] = delete