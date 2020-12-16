# intracluster
async def run(server):
    import asyncio, os
    from easyrpc.proxy import EasyRpcProxy

    # global variable to store proxy references to all 
    # pyql-cluster endpoints
    server.rpc_endpoints = {}

    log = server.log
    
    # local proxy connection to local EasyRpcServer
    #server.rpc[server.PYQL_NODE_ID]
    
    """
    server.rpc_endpoints[server.PYQL_NODE_ID] = await EasyRpcProxy.create(
        '0.0.0.0',
        '8190',
        '/ws/pyql-cluster',
        server_secret=await server.env['PYQL_LOCAL_SERVICE_TOKEN'],
        namespace=server.PYQL_NODE_ID
    )
    """

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def add_rpc_endpoint(host, port, path, secret, namespace):
        """
        adds proxy connectio to server.rcp_endpoints
        """
        server.rpc_endpoints[namespace] = await EasyRpcProxy.create(
            host,
            port,
            path,
            server_secret=secret,
            namespace=namespace
        )
        await server.rpc_endpoints[namespace]['add_reverse_rpc_endpoint'](
            os.environ['PYQL_NODE'],
            os.environ['PYQL_PORT'],
            '/ws/pyql-cluster',
            secret=await server.env['PYQL_LOCAL_SERVICE_TOKEN'],
            namespace=server.PYQL_NODE_ID
        )
    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def add_reverse_rpc_endpoint(host, port, path, secret, namespace):
        """
        attempts to add proxy connection only if non-existent
        """
        if not namespace in server.rpc_endpoints:
            server.rpc_endpoints[namespace] = await EasyRpcProxy.create(
                host,
                port,
                path,
                server_secret=secret,
                namespace=namespace
            )


    async def cluster_add_rpc_endpoint(host, port, path, secret, namespace):
        """
        invokes 'add_rpc_endpoint' on all proxy connection registered in server.rcp_endpoints
        """        
        for endpoint in server.rpc_endpoints:
            try:
                await server.rpc_endpoints[endpoint]['add_rpc_endpoint'](
                    host, port, path, secret, namespace
                )
            except KeyError:
                server.log.exception(f"add_rpc_endpoint not found in {server.rpc_endpoints[endpoint]}")


    server.cluster_add_rpc_endpoint = cluster_add_rpc_endpoint

    server.rpc_endpoints[server.PYQL_NODE_ID] = server.rpc[server.PYQL_NODE_ID]