# update
async def run(server):
    from fastapi import Request, Depends
    from pydantic import BaseModel



    @server.api_route('/db/{database}/table/{table}/update', methods=['POST'])
    async def db_update(database: str, table: str, request: Request,  params: dict = None, token: dict = Depends(server.verify_token)):
        return await db_update_auth(database, table, params=params, request=await server.process_request(request))
    @server.is_authenticated('local')
    async def db_update_auth(database, table, params=None, **kw):
        return await update(database, table, params, **kw)

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def update(database, table, params=None, **kw):
        message, rc = await server.check_db_table_exist(database,table)
        if not rc == 200:
            server.http_exception(rc, message)
        #200
        table = server.data[database].tables[table]
        if not 'set' in params or not 'where' in params: #TODO - Remove if pydantic model works with set: dict 
            server.http_exception(
                400,
                f"""missing key-values set: {'{"column_name": "value"}'} and where: {'{"column_name": "value"}'}"""
            )
        response = await table.update(**params['set'], where=params['where'])
        return {"message": "OK"}
        
    server.actions['update'] = update