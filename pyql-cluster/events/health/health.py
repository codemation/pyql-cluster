async def run(server):

    async def server_db_check():
        return await server.internal_db_check()
    server.tasks.append(
        server_db_check
    )
