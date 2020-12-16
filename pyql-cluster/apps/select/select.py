# select
async def run(server):
    from fastapi import Request, Depends
    from typing import Optional
    from pydantic import BaseModel

    class Select(BaseModel):
        selection: list = ['*', 'col1', 'col2', 'col3']
        where: Optional[dict] = {'col1': 'val'}

    log = server.log
    @server.api_route('/db/{database}/table/{table}/select', methods=['GET', 'POST'])
    async def select_func_api(
        database: str, 
        table: str, 
        request: Request, 
        params: Select = None, 
        token: dict = Depends(server.verify_token)
    ):
        return await select_func(database, table, params=params,  request=await server.process_request(request))
    @server.is_authenticated('local')
    async def select_func(database, table, **kw):
        request = kw['request']
        return await select(database, table, method=request.method, **kw)
    
    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def select(database,table, params=None, method='GET', **kw):
        message, rc = await server.check_db_table_exist(database,table)
        if not rc == 200:
            server.http_exception(
                500, 
                log.error(f"received non 200 rec with message {message}, rc {rc} during check_db_table_exist")
            )
        if method == 'GET' and params == None:
            response = await server.data[database].tables[table].select('*')
        else:
            if not 'select' in params:
                warning = f"table {table} select - missing selection"
                server.http_exception(400, log.warning(warning))
            p = {}
            select = params['select']
            if 'join' in params:
                p['join'] = params['join']
            if 'where' in params:
                p['where'] = params['where']
            if 'orderby' in params:
                p['orderby'] = params['orderby']
            response = await server.data[database].tables[table].select(*select, **p)
        if response == None or isinstance(response, str):
            server.http_exception(500, f"response object returned {response}")
        response = [response] if isinstance(response, dict) else response
        return {"data": response}
            
    server.actions['select'] = select