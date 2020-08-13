# update
async def run(server):
    import os
    from fastapi import Request
    if await server.env['SETUP_ID'] == server.setup_id:
        if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
            admin_id = await server.clusters.auth.select(
                'id', 
                where={'username': 'admin'}
            )
            service_id = await server.clusters.auth.select(
                'id', where={'parent': admin_id})
            



    @server.api_route('/db/{database}/table/{table}/update', methods=['POST'])
    async def db_update_api(database: str, table: str, request: Request,  params: dict = None):
        return await db_update(database, table, params=params, request=await server.process_request(request))
    @server.is_authenticated('local')
    async def db_update(database, table, params=None, **kw):
        return await update_func(database, table, params, **kw)
    async def update_func(database, table, params=None, **kw):
