
async def run(server):

    #### Create HTTPException Handler
    from fastapi import HTTPException, Request
    from easyrpc.server import EasyRpcServer
    import json, uuid, asyncio, random

    log = server.log

    # Reset SETUP_ID
    await server.env.set_item('SETUP_ID', 'UNSET')

    server.setup_id = str(uuid.uuid1())

    env_setup_id = await server.env['SETUP_ID']

    log.warning(f"ENV SETUP ID: {env_setup_id}")
    if await server.env['SETUP_ID'] in [None, 'UNSET']:
        await server.env.set_item('SETUP_ID', server.setup_id)

    if await server.env['SETUP_ID'] == server.setup_id:
        log.warning(f"SETUP_ID using {server.setup_id}")

    def http_exception(status, detail):
        raise HTTPException(status_code=status, detail=detail)
    server.http_exception = http_exception

    class RequestStorage:
        def __init__(self, url, headers, method, json_body):
            self.url = url
            self.headers = dict(headers)
            self.method = method
            self.json = json_body



    async def process_request(request: Request):
        json_body = None
        if 'content-length' in request.headers and request.headers['content-type'] == 'application/json':
            body = await request.body()
            json_body = json.loads(body) if len(body) > 0 else None
        return RequestStorage(
            request.url, 
            request.headers, 
            request.method, 
            json_body
        )
    server.process_request = process_request


    async def check_db_table_exist(database,table):
        if database in server.data:
            if not table in server.data[database].tables:
                await server.db_check(database)
            if table in server.data[database].tables:
                return "OK", 200
            else:
                return {'status': 404, 'message': f'table with name {table} not found in database {database}'}, 404   
        else:
            return {'status': 404, 'message': f'database with name {database} not found'}, 404
    server.check_db_table_exist = check_db_table_exist


    from apps.auth import auth
    await auth.run(server)

    server.rpc = EasyRpcServer(
        server, 
        f'/ws/pyql-cluster', 
        server_secret=await server.env['PYQL_LOCAL_SERVICE_TOKEN'],
        logger=log
    ) 

    from apps.select import select
    await select.run(server)            
            
    from apps.update import update
    await update.run(server)            
            
    from apps.delete import delete
    await delete.run(server)            
            
    from apps.insert import insert
    await insert.run(server)            
            
    from apps.table import table
    await table.run(server)            
            
    from apps.internal import internal
    await internal.run(server)
      
    from apps.cluster import cluster
    await cluster.run(server)

    from apps.intracluster import intracluster
    await intracluster.run(server)            
            
    