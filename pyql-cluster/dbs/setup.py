
async def run(server):
    server.data=dict()
    server.actions = dict()
    from dbs.database import database_db
    await database_db.run(server)