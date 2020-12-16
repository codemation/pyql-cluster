# database - type sqlite3
async def run(server):
    import os, time
    from easyrpc.tools.database import EasyRpcProxyDatabase

    if os.environ.get('PYQL_CLUSTER_MODE') == 'test':
        os.environ['PYQL_DB_HOST'] = 'localhost'
        os.environ['PYQL_RPC_PORT'] = '8191'
        os.environ['PYQL_DB_NAME'] = 'cluster'
        os.environ['PYQL_RPC_SECRET'] = 'abcd1234'

    
    DB_HOST = os.environ.get('PYQL_DB_HOST')
    RPC_PORT = os.environ.get('PYQL_RPC_PORT')
    RPC_SECRET = os.environ.get('PYQL_RPC_SECRET')
    RPC_PATH = os.environ.get('RPC_PATH')

    DB_NAME = os.environ.get('PYQL_DB_NAME')

    if not DB_NAME:
        DB_NAME = 'cluster'
    if not DB_HOST:
        DB_HOST = 'localhost'
    if not RPC_PORT:
        RPC_PORT = int(os.environ['PYQL_PORT'] + 1)

        

    log = server.log
    
    server.data[DB_NAME] = await EasyRpcProxyDatabase.create(
        DB_HOST, 
        RPC_PORT, 
        f'/ws/{DB_NAME}' if not RPC_PATH else RPC_PATH, 
        server_secret=RPC_SECRET,
        namespace=DB_NAME,
        debug=True
    )

    from . import setup
    await setup.attach_tables(server)
    log.info(f"finished attach_tables in db {DB_NAME}")