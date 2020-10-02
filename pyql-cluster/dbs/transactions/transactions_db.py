# database - type sqlite3
async def run(server):
    import os
    log = server.log

    print(f"databse_db event_loop: {server.event_loop}")

    @server.api_route('/internal/db/attach')
    async def transactions_attach_api():
        return await database_attach()
    async def transactions_attach():
        config=dict()
        os.environ['DB_NAME'] = 'transactions' # TODO - Add to env variables config later
        db_name = 'transactions'
        if 'PYQL_TYPE' in os.environ:
            if os.environ['PYQL_TYPE'] == 'K8S' or os.environ['PYQL_TYPE'] == 'DOCKER':
                db_location = os.environ['PYQL_VOLUME_PATH']
                config['database'] = f'{db_location}/{db_name}'
        else:
            with open('.cmddir', 'r') as projDir:
                for project_path in projDir:
                    config['database'] = f'{project_path}dbs/transactions/{db_name}'
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

    response = await transactions_attach()
    log.info(f"database_attach result: {response}")