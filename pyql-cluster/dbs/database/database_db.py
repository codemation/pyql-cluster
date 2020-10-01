# database - type sqlite3
async def run(server):
    import os
    from fastapi.testclient import TestClient
    from fastapi.websockets import WebSocket

    log = server.log

    @server.api_route('/internal/db/attach')
    async def database_attach_api():
        return await database_attach()
    async def database_attach():
        config=dict()
        os.environ['DB_NAME'] = 'cluster' # TODO - Add to env variables config later
        if 'PYQL_TYPE' in os.environ:
            if os.environ['PYQL_TYPE'] == 'K8S' or os.environ['PYQL_TYPE'] == 'DOCKER':
                db_name = os.getenv('DB_NAME').rstrip()
                db_location = os.environ['PYQL_VOLUME_PATH']
                config['database'] = f'{db_location}/{db_name}'
        else:
            with open('.cmddir', 'r') as projDir:
                for project_path in projDir:
                    db_name = os.getenv('DB_NAME').rstrip()
                    config['database'] = f'{project_path}dbs/database/{db_name}'
        config['logger'] = log
        if server.PYQL_DEBUG == True:
            config['debug'] = True

        from aiopyql import data
        from . import setup
        log.info("finished imports")
        server.data[db_name] = await data.Database.create(
            **config,
            loop=server.event_loop 
            )
        # enable database cache
        server.data[db_name].enable_cache()
        log.info("finished dbsetup")
        await setup.attach_tables(server)
        log.info("finished attach_tables")
        return {"message": "database attached successfully"}

    response = await database_attach()
    log.info(f"database_attach result: {response}")