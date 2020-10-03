async def run(server):

    import asyncio
    from datetime import datetime
    import time, uuid, random
    from random import randrange
    import json, os
    from collections import Counter

    from fastapi import Request
    from pydantic import BaseModel
    from aiohttp import ClientSession

    from apps.cluster.asyncrequest import async_request_multi, async_get_request, async_post_request
    from apps.cluster import tracer

    log = server.log

    #used to store links to functions called by jobs
    server.clusterjobs = {}

    # used for request session references
    server.sessions = {}

    server.trace = tracer.get_tracer(log)

    class Cluster:
        """
            object used for quickly referencing tables
            clusters, endpoints, databases, tables, state
        """
        def __init__(self, db_name):
            for table in server.data[db_name].tables:
                setattr(self, table, server.data[db_name].tables[table])

    server.clusters = Cluster('cluster')

    endpoints = await server.clusters.endpoints.select('*') if 'endpoints' in server.data['cluster'].tables else []

    while True:
        uuid_check = await server.data['cluster'].tables['pyql'].select('uuid', where={'database': 'cluster'})
        if len(uuid_check) > 0:
            for _,v in uuid_check[0].items():
                dbuuid = str(v)
            break
        else:
            if await server.env['SETUP_ID'] == server.setup_id:
                dbuuid = str(uuid.uuid1())
                await server.data['cluster'].tables['pyql'].insert({
                    'uuid': dbuuid,
                    'database': 'cluster', 
                    'last_mod_time': time.time()
                })
                await server.env.set_item('PYQL_ENDPOINT', dbuuid)
                break
            continue
    node_id = dbuuid
    

    os.environ['PYQL_ENDPOINT'] = dbuuid
    os.environ['HOSTNAME'] = '-'.join(os.environ['PYQL_NODE'].split('.'))

    if not 'PYQL_CLUSTER_ACTION' in os.environ:
        os.environ['PYQL_CLUSTER_ACTION'] = 'join'

    # Table created only if 'init' is passed into os.environ['PYQL_CLUSTER_ACTION']
    tables = []
    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        table_list = ['clusters', 'endpoints', 'tables', 'state', 'jobs', 'auth', 'data_to_txn_cluster']
        tables = [{table_name: await server.get_table_config('cluster', table_name)} for table_name in table_list]
    
    join_job_type = "node" if os.environ['PYQL_CLUSTER_ACTION'] == 'init' or len(endpoints) == 1 else 'cluster'

    join_cluster_job = {
        "job": f"{os.environ['HOSTNAME']}{os.environ['PYQL_CLUSTER_ACTION']}_cluster",
        "job_type": join_job_type,
        "method": "POST",
        "path": "/cluster/pyql/join",
        "data": {
            "name": os.environ['HOSTNAME'],
            "path": f"{os.environ['PYQL_NODE']}:{os.environ['PYQL_PORT']}",
            "token": await server.env['PYQL_LOCAL_SERVICE_TOKEN'],
            "database": {
                'name': "cluster",
                'uuid': dbuuid
            },
            "tables": tables,
            "consistency": ['clusters', 'endpoints', 'auth'] # defines whether table modifications are cached before submission & txn time is reviewed
        }
    }


    if 'PYQL_CLUSTER_JOIN_TOKEN' in os.environ and os.environ['PYQL_CLUSTER_ACTION'] == 'join':
        join_cluster_job['join_token'] = os.environ['PYQL_CLUSTER_JOIN_TOKEN']

    async def get_clusterid_by_name_authorized(cluster_name, **kwargs):
        log.warning(f"get_clusterid_by_name_authorized {kwargs}")
        if 'cluster_allowed' in kwargs:
            return kwargs['cluster_allowed']
        request = kwargs['request']
        user_id = request.auth
        log.warning(f"check_user_access called for cluster {cluster_name} using {user_id}")
        clusters = await server.clusters.clusters.select('*', where={'name': cluster_name})
        log.warning(f"get_clusterid_by_name_authorized clusters: {clusters}")
        cluster_allowed = None
        for cluster in clusters:
            log.warning(f"checking {user_id} {cluster['owner']}")
            if user_id == cluster['owner'] or user_id in cluster['access']['allow']:
                cluster_allowed = cluster['id']
                break
            if 'auth_children' in kwargs:
                for child_id in kwargs['auth_children']:
                    if child_id == cluster['owner'] or child_id in cluster['access']['allow']:
                        cluster_allowed = cluster['id']
                        break
        if cluster_allowed == None:
            env = kwargs
            warning = f"user {user_id} access to cluster with name {cluster_name}, no cluster was found which user has access rights or none exists - env {env}"
            server.http_exception(404, log.warning(warning))
        return str(cluster_allowed)
    server.get_clusterid_by_name_authorized = get_clusterid_by_name_authorized

    @server.trace
    async def get_auth_http_headers(location=None, token=None, **kw):
        trace=kw['trace']
        if token == None:
            auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not location == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
            trace.warning(f"get_auth_http_headers called using location: {location} - token: {token} - {kw} auth: {auth}")
            token = kw['token'] if 'token' in kw else None
            token = await server.env[auth] if token == None else token
        headers = {
            'Accept': 'application/json', "Content-Type": "application/json",
            "authentication": f"Token {token}"}
        trace.warning(f"get_auth_http_headers {headers}")
        return headers


    def cluster_name_to_uuid(func):
        """
        After authenticaion, uses 'userid' in server.auth to check
        if user has "access" to a cluster with name in kw['cluster']
        and replaces string name with 'uuid' of cluster if exists
        """
        async def check_user_access(*args, **kwargs):
            args = list(args)
            request = kwargs['request']
            log.warning(f"check_user_access - args {args} - {kwargs}")

            if 'cluster_allowed' in kwargs:
                if f"{kwargs['cluster_allowed']}_log" == args[0]:
                    args[0] = kwargs['cluster_allowed']
                if 'cluster' in kwargs and f"{kwargs['cluster_allowed']}_log" == kwargs['cluster']:
                    kwargs['cluster'] = kwargs['cluster_allowed']
                if 'table' in kwargs and kwargs['table'] == kwargs['table_allowed'] or args[1] == kwargs['table_allowed']:
                    return await func(*args, **kwargs)

            if not 'auth' in request.__dict__:
                log.error("authentication is required or missing, this should have been handled by is_authenticated")
                server.http_exception(500, "authentication is required or missing")
            
            cluster_name = kwargs['cluster'] if 'cluster' in kwargs else args[0]

            kwargs['cluster'] = await get_clusterid_by_name_authorized(
                cluster_name, **kwargs)
            args[0] = kwargs.pop('cluster') if cluster_name == args[0] else args[0]
            kwargs['cluster_name'] = cluster_name
            request.cluster_name = cluster_name
            args = tuple(args)
            return await func(*args, **kwargs)
        return check_user_access
    def state_and_quorum_check(func):
        """
        verifies that a pyql node is able to service requests otherwise tries to find a node which can
        Checks Requirements:
        - in_quorum - Continues only if in_quorum - no state checks if in_quorum = False
        - local state table is in_sync = True - servicing requests only if state table is in_sync True
        """
        async def state_quorum_safe_func(*args, **kwargs):
            request = kwargs['request']

            pyql = await server.env['PYQL_UUID']

            node_quorum = await server.clusters.quorum.select(
                '*',
                where={
                    'node': node_id
                }
            )
            state = await server.clusters.state.select(
                'state',
                where={
                    'name': f"{node_id}_state"
                }
            )

            if len(node_quorum) == 0 or node_quorum[0]['in_quorum'] == False:
                server.http_exception(
                    500,
                    log.error(f"cluster pyql node {os.environ['HOSTNAME']} is not in quorum {node_quorum}")
                )
            state = state[0]
            node_quorum = node_quorum[0]
            ready_and_healthy = node_quorum['health'] == 'healthy' and node_quorum['ready'] == True
            if state['state'] == 'loaded' and ready_and_healthy:
                kwargs['pyql'] = pyql
                return await func(*args, **kwargs)
            else:
                pyql = await server.env['PYQL_UUID'] if not 'pyql' in kwargs else kwargs['pyql']
                log.warning(f"node is inQuorum but not 'healthy' or state is not loaded: {node_quorum} {state}")
                pyql_nodes = await server.clusters.endpoints.select('uuid', 'path', where={'cluster': pyql})
                headers = dict(request.headers)
                # pop header fields which should not be passed
                for h in ['Content-Length']:
                    if h.lower() in headers:
                        headers.pop(h.lower())
                    if h in headers:
                        headers.pop(h)
                if not 'unsafe' in headers:
                    headers['unsafe'] = [node_id]
                else:
                    headers['unsafe'] = headers['unsafe'].split(',') + [node_id]
                for node in pyql_nodes:
                    if node['uuid'] in headers['unsafe']:
                        continue
                    if not node['uuid'] in node_quorum['nodes']:
                        log.warning(f"node {node} was not yet 'unsafe' but is not in_quorum - {node_quorum} -, marking unsafe and will try other, if any")
                        headers['unsafe'].append(node['uuid'])
                        continue
                    headers['Host'] = node['path']
                    url = request.url
                    headers['unsafe'] = ','.join(headers['unsafe'])
                    request_options = {
                        "method": request.method, 
                        "headers": headers, 
                        "data": request.json,
                        "session": await get_endpoint_sessions(node['uuid'])}
                    response, rc =  await probe(url, **request_options)
                    return response
                # Out of available - in_quorum nodes to try
                server.http_exception(500, log.error("No pyql nodes were available to service request"))
        state_quorum_safe_func.__name__ = '_'.join(str(uuid.uuid4()).split('-'))
        return state_quorum_safe_func
    server.state_and_quorum_check = state_and_quorum_check

    # setup auth apps which require server.state_and_quorum_check
    server.auth_post_cluster_setup(server)
    class PyqlUuid(BaseModel):
        PYQL_UUID: str


    @server.api_route('/pyql/setup', methods=['POST'])
    async def cluster_set_pyql_id_api(pyql_id: PyqlUuid, request: Request):
        return await cluster_set_pyql_id(dict(pyql_id),  request=await server.process_request(request))

    @server.is_authenticated('local')
    @server.trace
    async def cluster_set_pyql_id(pyql_id, **kw):
        return await set_pyql_id(pyql_id)

    async def set_pyql_id(pyql_id, **kw):
        await server.env.set_item('PYQL_UUID', pyql_id['PYQL_UUID'])
        
        return {'message': log.warning(f"updated PYQL_UUID with {pyql_id['PYQL_UUID']}")}

    @server.api_route('/cache/reset', methods=['POST'])
    async def cluster_node_reset_cache(reason: str, request: Request):
        return await node_reset_cache(reason,  request=await server.process_request(request))

    @server.is_authenticated('local')
    @server.trace
    async def node_reset_cache(reason, **kw):
        """
            resets local db table 'cache' 
        """
        trace=kw['trace']
        trace(f"cache reset called for {reason}")
        server.reset_cache()
        return {"message": f"{nodeId} reset_cache completed"} 
    server.node_reset_cache = node_reset_cache

    @server.trace
    async def get_endpoint_sessions(endpoint, **kw):
        """
        pulls endpoint session if exists else creates & returns
        """
        trace = kw['trace']
        loop = server.event_loop if not 'loop' in kw else kw['loop']
        async def session():
            async with ClientSession(loop=loop) as client:
                trace(f"started session for endpoint {endpoint}")
                while True:
                    status = yield client
                    if status == 'finished':
                        trace(f"finished session for endpoint {endpoint}")
                        break
        if not endpoint in server.sessions:
            server.sessions[endpoint] = [{'session': session(), 'loop': loop}]
            return await server.sessions[endpoint][0]['session'].asend(None)
        for client in server.sessions[endpoint]:
            if loop == client['loop']:
                return await client['session'].asend(endpoint)
        log.warning("session existed but not for this event loop, creating")
        client = session()
        server.sessions[endpoint].append({'session': client, 'loop': loop})
        return await client.asend(None)

    # attaching to global server var    
    server.get_endpoint_sessions = get_endpoint_sessions

    # creating initial session for this nodeq
    await get_endpoint_sessions(node_id)

    async def cleanup_session(endpoint):
        #try:
        if endpoint in server.sessions:
            log.warning(f"removing session for endpoint {endpoint}")
            try:
                for session in server.sessions[endpoint]:
                    try:
                        await session['session'].asend('finished')
                    except StopAsyncIteration:
                        pass
                del server.sessions[endpoint]
            except Exception as e:
                log.exception("exception when cleaning up session")
        #except StopAsyncIteration:
        #    await cleanup_session(endpoint)
        #    del server.sessions[endpoint]
        #return
    async def cleanup_sessions():
        for endpoint in server.sessions:
            await cleanup_session(endpoint)
        return

    @server.trace
    async def probe(path, method='GET', data=None, timeout=5.0, auth=None, headers=None, **kw):
        trace = kw['trace']
        auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
        headers = await get_auth_http_headers(auth, **kw) if headers == None else headers
        session, temp_session, temp_id = None, False, None
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

        if not 'session' in kw:
            temp_session, temp_id = True, str(uuid.uuid1())
            session = await get_endpoint_sessions(temp_id, loop=loop)
        else:
            session = kw['session']
            
        url = f'{path}'
        try:
            request = {
                'probe': {
                    'path': url,
                    'headers': headers,
                    'timeout': timeout
                }
            }
            if method == 'GET':
                result = await async_get_request(session, request, loop=loop) 
            else:
                request['probe']['data'] = data
                result = await async_post_request(session, request, loop=loop)
            result, status = result['probe']['content'], result['probe']['status']
        except Exception as e:
            error = f"Encountered exception when probing {path} - {repr(e)}"
            result, status = {"error": trace.error(error)}, 500
        trace(f"request: {request} - result: {result}")
        if temp_session:
            await cleanup_session(temp_id)
        return result, status
    server.probe = probe
    async def wait_on_jobs(pyql, cur_ind, job_list, waiting_on=None):
        """
            job queing helper function - guarantees 1 job runs after the other by creating "waiting jobs" 
             dependent on the first job completing
        """
        if len(job_list) > cur_ind + 1:
            job_list[cur_ind]['config']['nextJob'] = await wait_on_jobs(pyql, cur_ind+1, job_list)
        if cur_ind == 0:
            result = await jobs_add(job_list[cur_ind])
            return result['job_id']
        result = await jobs_add(job_list[cur_ind], status='waiting')
        return result['job_id']
    @server.trace
    async def bootstrap_cluster(cluster_id, name, config, **kw):
        """
            runs if this node is targeted by /cluster/pyql/join and pyql cluster does not yet exist
        """
        trace = kw['trace']
        trace.info(f"bootstrap starting for {config['name']} config: {config}")

        async def get_clusters_data():
            # ('id', str, 'UNIQUE NOT NULL'),
            # ('name', str),
            # ('owner', str), # UUID of auth user who created cluster 
            # ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            # ('created_by_endpoint', str),
            # ('create_date', str)
            admin_id = kw['authentication']
            service_id = await server.clusters.auth.select(
                'id', 
                where={'parent': admin_id})
            service_id = service_id[0]['id']

            return {
                'id': cluster_id,
                'name': name,
                'owner': service_id,
                'access': {'allow': [admin_id, service_id]},
                'type': 'data' if name == 'pyql' else 'log',
                'key': server.encode(
                    os.environ['PYQL_CLUSTER_INIT_ADMIN_PW'],
                    key=await server.env['PYQL_CLUSTER_TOKEN_KEY']
                    ) if name == 'pyql' else None,
                'created_by_endpoint': config['name'],
                'create_date': f'{datetime.now().date()}'
                }
        def get_endpoints_data(cluster_id):
            return {
                'id': str(uuid.uuid1()),
                'uuid': config['database']['uuid'],
                'db_name': config['database']['name'],
                'path': config['path'],
                'token': config['token'],
                'cluster': cluster_id
            }
        def get_databases_data(cluster_id):
            return {
                'name': f'{config["name"]}_{config["database"]["name"]}',
                'cluster': cluster_id,
                'uuid': config['database']['uuid'],
                'db_name': config['database']['name'],
                'endpoint': config['name']
            }
        def get_tables_data(table, cluster_id, cfg, consistency):
            return {
                'id': f"{cluster_id}_{table}",
                'name': table,
                'cluster': cluster_id,
                'config': cfg,
                'consistency': consistency,
                'is_paused': False
            }
        def get_state_data(table, cluster_id):
            return {
                'name': f'{config["database"]["uuid"]}_{table}',
                'state': 'loaded',
                'table_name': table,
                'cluster': cluster_id,
                'uuid': config['database']['uuid'], # used for syncing logs 
                'last_mod_time': time.time()
            }
        async def execute_request(endpoint, db, table, action, data):
            await server.data[db].tables[table].insert(**data)
        localhost = f'http://localhost:{os.environ["PYQL_PORT"]}'
        # cluster table
        clusterData = await get_clusters_data()
        await execute_request(localhost, 'cluster', 'clusters',
            'insert', clusterData)
        # endpoints
        await execute_request(localhost, 'cluster', 'endpoints',
            'insert', get_endpoints_data(clusterData['id']))
        # tables & state 
        for table in config['tables']:
            print(table)
            for table_name, cfg in table.items():
                r = await execute_request(
                    localhost,
                    'cluster',
                    'tables',
                    'insert',
                    get_tables_data(
                        table_name,
                        clusterData['id'], 
                        cfg,
                        table_name in config['consistency']
                        )
                )
        for table in config['tables']:
            for name, cfg in table.items():
                r = await execute_request(
                    localhost,  
                    'cluster',
                    'state',
                    'insert',
                    get_state_data(name, clusterData['id'])
                )
        trace.info("finished bootstrap")
    
    #No auth should be required 
    @server.api_route('/pyql/node')
    async def cluster_node():
        """
            returns node-id - to be used by workers instead of relying on pod ip:
        """
        log.warning(f"get node_id called {node_id}")
        return {"uuid": node_id}

    #TODO - Determine if I need to make pyql/ready authenticated 
    @server.api_route('/cluster/pyql/ready', methods=['POST', 'GET'])
    async def cluster_ready(request: Request, ready: dict = None):
        request = await server.process_request(request)
        if request.method == 'GET':
            quorum = await cluster_quorum_update()
            log.warning(f"readycheck - {quorum}")
            if "quorum" in quorum and quorum['quorum']["ready"] == True:
                return quorum['quorum']
            else:
                server.http_exception(400, quorum)
        else:
            """
                expects:
                ready =  {'ready': True|False}
            """
            if ready == None:
                server.http_exception(400, "missing input for ready")
            update_set = {
                'set': {'ready': ready['ready']}, 'where': {'node': node_id}
            }
            await server.clusters.quorum.update(
                **update_set['set'], where=update_set['where'])
            return ready
    @server.trace
    async def update_cluster_ready(path=None, ready=None, config=None, **kw):
        if not config == None:
            path, ready = config['path'], config['ready']
        return await probe(f"http://{path}/cluster/pyql/ready", method='POST', data={'ready': ready}, **kw)

    server.clusterjobs['update_cluster_ready'] = update_cluster_ready

    @server.trace
    async def cluster_endpoint_delete(cluster=None, endpoint=None, config=None, **kw):
        """
        This function is important to be used when node(s) goes down as the order in which
        a node comes up cannot always be guaranteed, so a node may come up with a different
        ip address
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        if not config == None:
            cluster, endpoint = config['cluster'], config['endpoint']
        trace.error(f"cluster_endpoint_delete called for cluster - {cluster}, endpoint - {endpoint}")
        delete_where = {'where': {'uuid': endpoint, 'cluster': cluster}}
        results = {}
        results['state'] = await cluster_table_change(pyql, 'state', 'delete', delete_where, **kw)
        results['endpoints'] = await cluster_table_change(pyql, 'endpoints', 'delete', delete_where, **kw)
        return {"message": trace(f"deleted {endpoint} successfully - results {results}")}
    server.clusterjobs['cluster_endpoint_delete'] = cluster_endpoint_delete

    @server.trace
    async def get_alive_endpoints(endpoints, timeout=2.0, **kw):
        trace = kw['trace']
        loop = server.event_loop if not 'loop' in kw else kw['loop']

        trace(f"starting - checking endpoints: {endpoints}")

        ep_requests = {}
        for _endpoint in endpoints:
            endpoint = _endpoint if isinstance(endpoints, list) else endpoints[_endpoint]
            if endpoint['uuid'] == node_id:
                # no need to check own /pyql/node 
                continue 
            ep_requests[endpoint['uuid']] = {
                'path': f"http://{endpoint['path']}/pyql/node",
                'timeout':timeout,
                'session': await get_endpoint_sessions(endpoint['uuid'], **kw)
            }
        ep_results = await async_request_multi(ep_requests, loop=loop)
        for endpoint_id, response in ep_results.items():
            if response['status'] == 408:
                log.warning(f"observed timeout with endpoint {endpoint_id}, triggering cleanup of session")
                await cleanup_session(endpoint_id)
        ep_results[node_id] = {'status': 200, 'content': "SELF"}
        trace.warning(f"get_alive_endpoints - {ep_results}")
        return ep_results

    @server.api_route('/pyql/quorum/check', methods=['POST'])
    async def cluster_quorum_refresh(request: Request):
        return await quorum_refresh( request=await server.process_request(request))
    @server.is_authenticated('pyql')
    @server.trace
    async def quorum_refresh(**kw):
        return await cluster_quorum_check(trace=kw['trace'])

    @server.trace
    async def cluster_quorum_check(**kw):
        trace = kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace.warning(f"received cluster_quorum_check for cluster {pyql}")
        pyql_endpoints = await server.clusters.endpoints.select('*', where={'cluster': pyql})
        if len(pyql_endpoints) == 0:
            return {
                "message": trace.warning(
                    "cluster_quorum_check found no pyql_endpoints, cluster may still be initializing")
                    }
        quorum = await server.clusters.quorum.select('*')
        # Check which pyql_endpoints are alive   
        alive_endpoints = await get_alive_endpoints(pyql_endpoints, **kw)
        alive_endpoints_nodes = [node_id]
        for endpoint in alive_endpoints:
            if alive_endpoints[endpoint]['status'] == 200:
                alive_endpoints_nodes.append(endpoint)
        # Compare live endpoints to current quorum 
        latestQuorumNodes = quorum[0]['nodes']
        if len(alive_endpoints_nodes) / len(pyql_endpoints) < 2/3: 
            quorum = {'alive': alive_endpoints_nodes, 'members': pyql_endpoints}
            server.http_exception(
                500, 
                trace.warning(f" detected node {node_id} is outOfQuorum - quorum {quorum}"))

        ep_requests = {}
        ep_list = []
        for endpoint in pyql_endpoints:
            # only trigger a pyql/quorum update on live endpoints
            if endpoint['uuid'] in alive_endpoints_nodes:
                ep_list.append(endpoint['uuid'])
                endpoint_path = endpoint['path']
                endpoint_path = f'http://{endpoint_path}/pyql/quorum'
                ep_requests[endpoint['uuid']] = {
                    'path': endpoint_path, 'data': None, 'timeout': 5.0,
                    'headers': await get_auth_http_headers('remote', token=endpoint['token']),
                    'session': await get_endpoint_sessions(endpoint['uuid'], **kw)
                    }

        trace.warning(f"cluster_quorum_check - running using {ep_requests}")
        if len(ep_list) == 0:
            return {"message": f"pyql node {node_id} is still syncing"}
        try:
            ep_results = await async_request_multi(ep_requests, 'POST', loop=kw['loop'])
        except Exception as e:
            trace.exception("Excepton found during cluster_quorum() check")
        trace.warning(f"cluster_quorum_check - results {ep_results}")

        return {
            "message": trace(f"cluster_quorum_check completed on {node_id}"), 
            'results': ep_results
            }
    server.clusterjobs['cluster_quorum_check'] = cluster_quorum_check
        
    @server.api_route('/pyql/quorum', methods=['GET', 'POST'])
    async def cluster_quorum_query_api(request: Request):
        return await cluster_quorum_query_auth(request=await server.process_request(request))
    @server.is_authenticated('local')
    @server.trace
    async def cluster_quorum_query_auth(check=False, get=False, **kw):
        trace=kw['trace']
        request = kw['request'] if 'request' in kw else None
        if request and request.method == 'POST':
            return await cluster_quorum_update(trace=kw['trace'])
        return await cluster_quorum_query()

    @server.trace
    async def cluster_quorum_query(check=False, get=False, **kw):
        quorum = await server.clusters.quorum.select(
            '*', where={'node': node_id})
        return {'quorum': quorum[0]}

    @server.trace
    async def cluster_quorum_update(**kw):
        trace = kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        pyql_endpoints = await server.clusters.endpoints.select(
            '*', 
            where={'cluster': pyql}
        )

        if len(pyql_endpoints) == 0:
            return {"message": trace(f"cluster_quorum_update node {node_id} is still syncing")}
        if len(pyql_endpoints) == 1:
            health = 'healthy'
        # get last recorded quorum - to check against
        last_quorum = await server.clusters.quorum.select(
            '*', 
            where={'node': node_id}
        )
        last_quorum = last_quorum[0]
        trace(f"last_quorum  - {last_quorum}")

        # check alive endpoints
        alive_endpoints = await get_alive_endpoints(pyql_endpoints, **kw)
        trace(f"alive_endpoints - {alive_endpoints}")

        # build list of in_quorum & missing nodes

        in_quorum_nodes, missing_nodes = {}, {}
        for endpoint in alive_endpoints:
            if alive_endpoints[endpoint]['status'] == 200:
                in_quorum_nodes[endpoint] = time.time()
            else:
                if not endpoint in last_quorum['missing']:
                    missing_nodes[endpoint] = time.time()
                else:
                    missing_nodes[endpoint] = last_quorum['missing'][endpoint]

        quorum_to_set = {
            'in_quorum': False,
            'nodes': in_quorum_nodes,
            'missing': missing_nodes
            }
        if len(in_quorum_nodes) / len(pyql_endpoints) >= 2/3:
            trace(f"node is in_quorum=True")
            quorum_to_set['in_quorum'] = True
            if last_quorum['ready'] == False and last_quorum['health'] == 'unhealthy':
                trace(f"last_quorum was ready=False, will mark endpoint 'healing'")
                quorum_to_set['health'] = 'healing'
                # create healing job - which marks endpoint stale
                # on all pyql endpoints, thus triggering re-sync & 
                # will mark endpoint ready upon healing
                job = {
                    'job': f"{node_id}_mark_state_stale",
                    'job_type': 'cluster',
                    'action': 'table_update',
                    'config': {
                        'cluster': pyql, 
                        'table': 'state', 
                        'data': {
                            'set': {
                                'state': 'stale',
                                'info': {
                                    'stale reason': 'endpoint was out_of_quorum and started healing',
                                    'operation': trace.get_root_operation(),
                                    'node': node_id
                                }
                            },
                            'where': {
                                'uuid': node_id
                            }
                        }
                    }
                }
                add_healing_job_result = await jobs_add(job, **kw)
                trace(f"add_healing_job_result - {add_healing_job_result}")
            else:
                if last_quorum['health'] == 'healing':
                    trace(f"last_quorum is ready=True and health='healing', marking helathy")
                    quorum_to_set['health'] = 'healthy'
        else:
            trace(f"node is in_quorum=False")
            if last_quorum['ready'] == True:
                trace(f"last_quorum ready=True, so will mark this endpoint ready=False")
                quorum_to_set['ready'] = False
            # mark un-healhty if healing or healthy
            if last_quorum['health'] in {'healthy', 'healing'}:
                trace(f"last_quorum health = healthy|healing, so will mark this endpoint health='unhealthy")
                quorum_to_set['health'] = 'unhealthy'
                # mark all endpoints stale for this node
                await server.clusters.state.update(
                    loaded='stale',
                    info={
                        'stale reason': 'endpoint became un-healthy',
                        'operation': trace.get_root_operation(),
                        'node': node_id
                    },
                    where={
                        'uuid': node_id
                    }
                )
        trace(f"quorum_to_set - {quorum_to_set}")
        await server.clusters.quorum.update(
            **quorum_to_set,
            where={
                'node': node_id
            }
        )
        updated_quorum = await server.clusters.quorum.select(
            '*',
            where={'node': node_id}
        )
        trace(f"current_quorum - {updated_quorum}")
        return {"quorum": updated_quorum[0]}
    server.clusterjobs['cluster_quorum_update'] = cluster_quorum_update

    @server.trace
    async def cluster_quorum(update=False, **kw):
        trace = kw['trace']
        if update == True:
            await cluster_quorum_update(trace=trace)
        quorum_select = await server.clusters.quorum.select('*', where={'node': node_id})
        return {'quorum': quorum_select[0]}
   
    @server.api_route('/cluster/{cluster}/table/{table}/path')
    async def cluster_get_db_table_path(cluster: str, table: str, request: Request):
        return await get_db_table_path(cluster, table,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def get_db_table_path(cluster, table, **kw):
        paths = {'loaded': {}, 'stale': {}, 'new': {}}
        table_endpoints = await get_table_endpoints(cluster, table, trace=kw['trace'])
        tb = await get_table_info(cluster, table, table_endpoints, trace=kw['trace'])
        for pType in paths:
            for endpoint in table_endpoints[pType]:
                #dbName = get_db_name(cluster, endpoint)
                dbName = tb['endpoints'][f'{endpoint}{table}']['db_name']
                paths[pType][endpoint] = tb['endpoints'][f'{endpoint}{table}']['path']
        return paths

    @server.api_route('/cluster/{cluster}/table/{table}/endpoints')
    async def cluster_get_table_endpoints_api(cluster: str, table: str, request: Request):
        return await cluster_get_table_endpoints(cluster, table,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_get_table_endpoints(cluster, table, **kw):
        cluster_name = kw['request'].__dict__.get('cluster_name')
        return await get_table_endpoints(cluster, table,cluster_name=cluster_name, **kw)

    @server.trace
    async def get_table_endpoints(cluster, table, cluster_name=None, **kw):
        trace = kw['trace']

        exclude = [] if not 'exclude' in kw else [kw['exclude']] 

        table_endpoints = {'loaded': {}, 'new': {}, 'stale': {}}

        # Get cluster Name - coro
        _cluster = server.clusters.clusters.select(
            'name',
            'type',
            where={'id': cluster}
        )

        # Get endpoints in cluster - coro
        endpoints = server.clusters.endpoints.select(
            '*', 
            join={
                'state': {
                    'endpoints.uuid': 'state.uuid', 
                    'endpoints.cluster': 'state.cluster'}
            }, 
            where={
                'state.cluster': cluster, 
                'state.table_name': table
            }
        )

        # Run Coros
        _cluster, endpoints = await asyncio.gather(
            _cluster,
            endpoints
        )
        _cluster = _cluster[0]

        # process {"<table>.<column>": <value>} into {"<column>": <value>} 
        endpoints_key_split = []
        for endpoint in endpoints:
            if endpoint['endpoints.uuid'] in exclude:
                trace(f"endpoint {endpoint} excluded")
                continue
            renamed = {}
            for k,v in endpoint.items():
                renamed[k.split('.')[1]] = v
            endpoints_key_split.append(renamed)

        for endpoint in endpoints_key_split:
            state = endpoint['state']
            table_endpoints[state][endpoint['uuid']] = endpoint
        table_endpoints['cluster_name'] = _cluster['name']
        table_endpoints['cluster_type'] = _cluster['type']
        trace.warning(f"result {table_endpoints}")
        return table_endpoints

    @server.trace
    def get_endpoint_url(path, action, **kw):
        trace = kw['trace']
        cache_path = '/cache/'.join(path.split('/table/'))
        if 'commit' in kw or 'cancel' in kw:
            action = 'commit' if 'commit' in kw else 'cancel'
            return trace(f'{cache_path}/txn/{action}')
        if 'cache' in kw:
            return trace(f'{cache_path}/{action}/{kw["cache"]}')
        else:
            return trace(f'{path}/{action}')

    @server.trace
    async def get_table_info(cluster, table, endpoints, **kw):
        trace = kw['trace']

        tb = await server.clusters.tables.select(
            '*',
            where={
                'id': f"{cluster}_{table}"
            }
        )
        if len(tb) == 0:
            server.http_exception(500, trace(f"no tables in cluster {cluster} with name {table} - found: {tables}"))
        tb = tb[0]
        trace(f"get_table_info_get_tables {tb}")
        tb['endpoints'] = {}
        for state in ['loaded', 'stale', 'new']:
            for endpoint in endpoints[state]:
                path = endpoints[state][endpoint]['path']
                db = endpoints[state][endpoint]['db_name']
                name = endpoints[state][endpoint]['name']
                tb['endpoints'][name] = endpoints[state][endpoint]
                tb['endpoints'][name]['path'] = f"http://{path}/db/{db}/table/{tb['name']}"
        trace(f"completed {tb}")
        return tb
        

    @server.trace
    async def cluster_table_read(cluster, table, data, **kw):
        """
        # Reads
        1. Check 'table shard cluster' associated with cluster
        2. Flush changes, to target endpoint, if any
        3. Read & Return 
        """
        trace = kw['trace']
        _txn = {action: request_data}
        trace(f"called for {_txn}")
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

    @server.trace
    async def pyql_state_tables_change(table, action, request_data, force=False, **kw):
        """
        Meant to be called for pyql cluster - tables ['tables', 'state', 'jobs']
        as logs will not be generated for these tables, but applied ASAP 
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

        if not force:
            # check if table is paused
            cur_wait, max_wait = 0.01, 10.0
            while cur_wait < max_wait:
                pause_check = await server.clusters.tables.select(
                    'is_paused',
                    where={
                        'id': f'{pyql}_{table}'
                    }
                )
                if pause_check[0]['is_paused'] == False:
                    break
                # wait an try again
                await asyncio.sleep(0.01)
                cur_wait+=0.01
            else:
                return {"error": trace.error(f"timeout reached waiting for {table} to un-pause")}

        table_endpoints = await get_table_endpoints(pyql, table, **kw)

        state_change_requests = {}

        for endpoint in table_endpoints['loaded']:
            _endpoint = table_endpoints['loaded'][endpoint]
            path = _endpoint['path']
            db = _endpoint['db_name']
            token = _endpoint['token']
            epuuid = _endpoint['uuid']

            state_change_requests[endpoint] = {
                'path': f"http://{path}/db/{db}/table/{table}/{action}",
                'data': request_data,
                'timeout': 2.0,
                'headers': await get_auth_http_headers('remote', token=token),
                'session': await get_endpoint_sessions(epuuid, **kw)
            }
        state_change_results = await async_request_multi(
            state_change_requests, 
            'POST', 
            loop=loop
        )

        status = {'success': [], 'fail': []}
        for _endpoint in state_change_results:
            endpoint = table_endpoints['loaded'][_endpoint]
            if state_change_results[_endpoint]['status'] == 200:
                status['success'].append(endpoint)
                continue
            status['fail'].append(endpoint)

        trace(f"state_change_results status: {status}")

        if len(status['success']) == 0:
            return {
                'error': trace.error("all endpoints failed"),
                'status': status
            }
       
        for fail_endpoint in status['fail']:
            mark_stale = {
                "set": {
                    'state': 'stale',
                    'info': {
                        'stale reason': f'{table} {action} failed',
                        'operation': trace.get_root_operation(),
                        'node': node_id
                        }
                },
                'where': {
                    'name': f"{fail_endpoint['uuid']}_{table}"
                }
            }

            if not table == 'state':
                trace(f"marking fail_endpoint {fail_endpoint['uuid']} stale")
                await pyql_state_tables_change(
                    'state',
                    'update',
                    mark_stale,
                    **kw
                )
                continue

            ## state table ##
            state_mark_stale = {}
            for endpoint in status['success']:
                path = endpoint['path']
                db = endpoint['db_name']
                token = endpoint['token']
                epuuid = endpoint['uuid']

                state_mark_stale[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/state/update",
                    'data': mark_stale,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            state_mark_stale_results = await async_request_multi(
                state_change_requests, 
                'POST', 
                loop=loop
            )
            trace(f"state_mark_stale_results: {state_mark_stale_results}")
        return {
            "state_change_results": state_change_results, 
            "status": status

        }
    @server.trace
    async def cluster_table_change(cluster, table, action, request_data, **kw):
        """
        called for table modifications 
        action: insert, update, delete 
        1. Check 'table shard cluster' associated with  cluster
        2. Write txn to 'table shard cluster'
        3. Return Success 

        Requirements:
        - Node processing request must be in_quorum
        - Node triggering table_change  
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']

        if cluster == pyql and table in ['jobs', 'state', 'tables']:
            return await pyql_state_tables_change(
                table,
                action,
                request_data, 
                **kw
            )

        _txn = {action: request_data}
        trace(f"called for {_txn}")
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

        # Check 'table txn log cluster' associated with  cluster
        txn_time = float(time.time())
        txn_cluster = await server.clusters.data_to_txn_cluster.select(
            'txn_cluster_id',
            'txn_cluster_name',
            where={'data_cluster_id': cluster}
        )
        txn_cluster_id = txn_cluster[0]['txn_cluster_id']
        txn_cluster_name = txn_cluster[0]['txn_cluster_name']
        txn = {
            "timestamp": time.time(),
            "txn": _txn
        }
        cluster_id_underscored = (
            '_'.join(cluster.split('-'))
        )

        # Create Task to signal endpoints 
        @server.trace
        async def signal_table_endpoints(**kw):
            trace = kw['trace']
            trace("starting")
            loop = asyncio.get_running_loop()
            kw['loop'] = loop
            table_endpoints = await get_table_endpoints(cluster, table, loop=loop)
            flush_requests = {}
            flush_tasks = []
            flush_requests_results = {}
            for endpoint in table_endpoints['loaded']:
                _endpoint = table_endpoints['loaded'][endpoint]
                path = _endpoint['path']
                db = _endpoint['db_name']
                token = _endpoint['token']
                epuuid = _endpoint['uuid']

                tx_table = '_'.join(f"txn_{cluster}_{table}".split('-'))
                
                # create limited use token
                limited_use_token = await server.create_auth_token(
                    cluster, # id
                    time.time() + 300, # 5 Minutes
                    'cluster',
                    extra_data={
                        'cluster_allowed': txn_cluster_id,
                        'table_allowed': tx_table
                    }
                )
                op = 'flush'
                flush_config = {
                    "tx_cluster_path": (
                        f"http://{os.environ['PYQL_CLUSTER_SVC']}/cluster/{txn_cluster_id}_log/table/{tx_table}/select"
                    ),
                    "token": limited_use_token
                }
                if epuuid == node_id:
                    async def local_flush():
                        flush_requests_results[endpoint] = {
                            'status': 200,
                            'content': await server.table_flush_trigger(
                                db,
                                table,
                                flush_config
                            )
                        }
                    flush_tasks.append(local_flush())
                    continue
                    
                #if cluster == pyql and table in ('jobs', 'state', 'tables'):
                #    op = action
                #    flush_config = txn['txn'][op]

                flush_requests[endpoint] = {
                    'path': f"http://{path}/db/{db}/table/{table}/{op}",
                    'data': flush_config,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            if flush_requests:
                async def remote_flush():
                    flush_requests_results.update(
                        await async_request_multi(flush_requests, 'POST', loop=loop)
                    )
                flush_tasks.append(remote_flush())
            await asyncio.gather(*flush_tasks, return_exceptions=True)
            
            # handle flush trigger failures by marking stale
            flush_fail_mark_stale = []
            for endpoint in flush_requests_results:
                if flush_requests_results[endpoint]['status'] == 200:
                    continue
                state_data = {
                    "set": {
                        "state": 'stale',
                        'info': {
                            'stale reason': 'flush trigger failed',
                            'operation': trace.get_root_operation(),
                            'node': node_id
                            }
                    },
                    "where": {
                        "name": f"{endpoint}_{table}"
                    }
                }
                flush_fail_mark_stale.append(
                    cluster_table_change(
                            pyql,
                            'state',
                            'update',
                            state_data,
                            **kw
                        )
                )
            if len(flush_fail_mark_stale) > 0:
                mark_stale_results = await asyncio.gather(
                    *flush_fail_mark_stale, 
                    loop=loop, 
                    return_exceptions=True
                )
                trace(f"flush trigger failure(s) detected, marking failed endpoint(s) stale - result {mark_stale_results}")

            return trace(f"flush_requests_results: {flush_requests_results}")

        #if not (cluster == pyql and table in ('jobs', 'state', 'tables')):
            # write to txn logs
        result = await write_to_txn_logs(
            txn_cluster_id,
            f"txn_{cluster_id_underscored}_{table}",
            txn,
            **kw
        )
        """
        # add task to txn worker queue
        if cluster == pyql and table == 'jobs':
            await signal_table_endpoints(**kw)
        else:
        """
        server.txn_signals.append(
            (
                signal_table_endpoints(**kw), # to be awaited by txn_signal workers
                signal_table_endpoints, # to be retried, if first coro fails
                kw # to be passed into any retries
            )
        )

        return {"result": trace("finished")}
        
    @server.trace
    async def write_to_txn_logs(log_cluster, log_table, txn, force=False, **kw):
        trace = kw['trace']
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        pyql_under = '_'.join(pyql.split('-'))

        stale_state_log_table = False if not 'stale_state_log_table' in kw else True

        if not force:
            # check if table is paused
            cur_wait, max_wait = 0.01, 10.0
            while cur_wait < max_wait:
                pause_check = await server.clusters.tables.select(
                    'is_paused',
                    where={
                        'id': f'{log_cluster}_{log_table}'
                    }
                )
                if pause_check[0]['is_paused'] == False:
                    break
                # wait an try again
                await asyncio.sleep(0.01)
                cur_wait+=0.01
            else:
                return {"error": trace.error(f"timeout reached waiting for {log_table} to un-pause")}

        table_endpoints = await get_table_endpoints(
            log_cluster, 
            log_table, 
            **kw
        )
        log_inserts = {}
        for endpoint in table_endpoints['loaded']:
            log_endpoint = table_endpoints['loaded'][endpoint]
            token = log_endpoint['token']
            db = log_endpoint['db_name']
            path = log_endpoint['path']
            ep_uuid = log_endpoint['uuid']
            request_path = f"http://{path}/db/{db}/table/{log_table}/insert"
            log_inserts[endpoint] = {
                'path': request_path,
                'data': txn,
                'timeout': 2.0,
                'headers': await get_auth_http_headers('remote', token=token),
                'session': await get_endpoint_sessions(endpoint, **kw)
            }
        log_insert_results = await async_request_multi(
            log_inserts, 
            'POST', 
            loop=loop
        )
        trace(f"log_insert_results: {log_insert_results}")

        pass_fail = Counter({'pass': 0, 'fail': 0})
        log_state_out_of_sync = []
        for endpoint in log_insert_results:
            if not log_insert_results[endpoint]['status'] == 200:
                pass_fail['fail']+=1
                if not stale_state_log_table:
                    state_data = {
                        "set": {
                            "state": 'stale',
                            'info': {
                                'stale reason': 'log insertion failed',
                                'operation': trace.get_root_operation(),
                                'node': node_id
                                }
                        },
                        "where": {
                            "name": f"{endpoint}_{log_table}"
                        }
                    }
                    if log_table == f'txn_{pyql_under}_state':
                        kw['stale_state_log_table'] = True
                    log_state_out_of_sync.append(
                        await cluster_table_change(
                                pyql,
                                'state',
                                'update',
                                state_data,
                                **kw
                            )
                    )
            else:
                pass_fail['pass']+=1

        # mark failures out_of_sync if sucesses & failures exist
        if pass_fail['fail'] > 0 and pass_fail['success'] > 0:
            trace(f"log insertion failure detected, marking failed endpoints stale - result {log_state_out_of_sync}")

        return {'results': log_insert_results, 'pass_fail': pass_fail}

    @server.trace
    async def table_select(cluster, table, **kw):
        return await endpoint_probe(cluster, table, '/select', **kw)
    server.cluster_table_select = table_select

    @server.trace
    async def get_random_table_endpoint(cluster, table, quorum=None, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']

        endpoints = await get_table_endpoints(cluster, table, **kw)
        endpoints = endpoints['loaded']
        loaded_endpoints = [ep for ep in endpoints]

        while len(loaded_endpoints) > 0: 
            if node_id in loaded_endpoints:
                endpoint_choice = loaded_endpoints.pop(loaded_endpoints.index(node_id))
            else:
                if len(loaded_endpoints) > 1:
                    endpoint_choice = loaded_endpoints.pop(randrange(len(loaded_endpoints)))
                else:
                    endpoint_choice = loaded_endpoints.pop(0)
            if not quorum == None and cluster == pyql:
                if not endpoint_choice in quorum['quorum']['nodes']:
                    trace.warning(f"get_random_table_endpoint skipped pyql endpoint {endpoint_choice} as not in quorum")
                    if len(loaded_endpoints) == 0 and table == 'jobs':
                        await pyql_reset_jobs_table(**kw)   
                        endpoints = await get_table_endpoints(cluster, table, **kw)
                        endpoints = endpoints['loaded']
                        loaded_endpoints = [ep for ep in endpoints]
                    continue
            yield endpoints[endpoint_choice]
        yield None
    #table_select(pyql, 'jobs', data=job_select, method='POST', **kw)    
    @server.trace
    async def endpoint_probe(cluster, table, path='', data=None, timeout=1.0, quorum=None, **kw):
        trace = kw['trace']
        request = kw['request'] if 'request' in kw else None
        errors = []

        if data == None:
            if not request == None:
                method = request.method if not 'method' in kw else kw['method']
            else:
                method = 'GET'
        else:
            if request == None or not 'method' in kw:   
                method = 'POST'
            else:
                method = request.method if not request == None else kw['method']

        if not 'method' in kw:
            kw['method'] = method if request == None else request.method
        
        if method in ['POST', 'PUT'] and data == None:
            server.http_exception(400, trace.error("expected json input for request"))
        async for endpoint in get_random_table_endpoint(cluster, table, quorum, **kw):
            if endpoint == None:
                server.http_exception(
                    500, 
                    trace(f"no in_sync endpoints in cluster {cluster} table {table} or all failed - errors {errors}")
                )
            try:
                if endpoint['uuid'] == node_id:
                    # local node, just use local select
                    if path == '' or path == '/select': # table select
                        return await server.actions['select'](endpoint['db_name'], table, params=data, method=method)
                    if path == '/config': # table config pull
                        return await server.get_table_config(endpoint['db_name'], table)
                    return await server.actions['select_key'](endpoint['db_name'], table, path[1:])
                url = f"http://{endpoint['path']}/db/{endpoint['db_name']}/table/{table}{path}"
                r, rc = await probe(
                    url,
                    data=data,
                    token=endpoint['token'],
                    timeout=timeout,
                    session=await get_endpoint_sessions(endpoint['uuid'], **kw),
                    **kw
                )
                if not rc == 200:
                    errors.append({endpoint['name']: trace.exception(f"non 200 rc encountered with {endpoint} {rc}")})
                    # Continue to try other endpoints
                    continue
                # Response OK returning values
                return r
            except Exception as e:
                errors.append({endpoint['name']: trace.exception(f"exception encountered with {endpoint}")})
                continue

    @server.api_route('/cluster/{cluster}/table/{table}', methods=['GET', 'PUT', 'POST'])
    async def cluster_table(cluster: str, table:str, request: Request, data: dict = None):
        return await cluster_table(cluster, table, data=data,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table(cluster, table, **kw):
        trace = kw['trace']
        request = kw['request']
        if request.method == 'GET':
            return await endpoint_probe(cluster, table, **kw)
        return table_insert(cluster, table, **kw)


    @server.api_route('/cluster/{cluster}/table/{table}/create', methods=['POST'])
    async def cluster_table_create_api(cluster: str, table: str, request: Request, config: dict):
        return await cluster_table_create_auth(
            cluster, 
            table, 
            request=await server.process_request(request),
            config=config
        )
    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_create_auth(cluster: str, table: str, config: dict, **kw):
        return await cluster_table_create(
            cluster,
            table,
            config, 
            **kw
        )
    @server.trace
    async def cluster_table_create(cluster: str, table: str, config: dict, **kw):
        pyql = await server.env['PYQL_UUID']
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        trace = kw['trace']

        # check existence of table 

        tables = await server.clusters.tables.select(
            'name',
            where={
                'cluster': cluster, 
                'name': table
            }
        )

        if len(tables) > 0:
            return {"message": f"table already exists"}
        
        endpoints = await server.clusters.endpoints.select(
            '*',
            where={'cluster': cluster}
        )

        ep_requests = {}
        for endpoint in endpoints:
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']
            token = endpoint['token']

            ep_requests[epuuid] = {
                'path': f"http://{path}/db/{db}/table/{table}/create",
                'data': config,
                'timeout': 2.0,
                'headers': await get_auth_http_headers('remote', token=token),
                'session': await get_endpoint_sessions(epuuid, **kw)
            }
        async_results = await async_request_multi(ep_requests, 'POST', loop=loop)

        # add tables to pyql tables

        new_table = {
            'id': f"{cluster}_{table}",
            'name': table,
            'cluster': cluster,
            'config': config,
            'is_paused': False
        }

        await cluster_table_change(pyql, 'tables', 'insert', new_table, **kw)

        # update state table
        update_state_tasks = []
        for endpoint in async_results:
            sync_state = 'loaded' if async_results[endpoint]['status'] == 200 else 'new'
            state_data = {
                'name': f"{endpoint}_{table}",
                'state': 'loaded',
                'table_name': table,
                'cluster': cluster,
                'uuid': endpoint # used for syncing logs
            }
            update_state_tasks.append(
                cluster_table_change(pyql, 'state', 'insert', state_data, **kw)
            )
        update_state_results = await asyncio.gather(
            *update_state_tasks, 
            loop=loop, 
            return_exceptions=True
        )
        trace(f"update_state_results - {update_state_results}")

        return {
            "message": trace(f"cluster {cluster} table {table} created - finished")
            }
    
    @server.trace
    async def cluster_txn_table_create(txn_cluster_id: str, data_cluster_and_table: str, **kw):
        """
        txn table name is composed of the following
        txn_{data_cluster_id}_{table_name}
        """
        txn_cluster_id_underscored = (
            '_'.join(txn_cluster_id.split('-'))
        )
        config = {
            f'txn_{data_cluster_and_table}': {
                "columns": [
                    {
                        "name": "timestamp",
                        "type": "float",
                        "mods": "UNIQUE"
                    },
                    {
                        "name": "txn",
                        "type": "str",
                        "mods": ""
                    }
                ],
                "primary_key": "timestamp", # "foreign_keys": None,
                "cache_enabled": True
            }
        }

        return await cluster_table_create(
            txn_cluster_id,
            f"txn_{data_cluster_and_table}",
            config,
            **kw
        )

    @server.api_route('/cluster/{cluster}/table/{table}/select', methods=['GET','POST'])
    async def cluster_table_select_api(cluster: str, table: str, request: Request, data: dict = None):
        return await cluster_table_select(cluster, table, data=data,  request=await server.process_request(request))
    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_select(cluster, table, data=None, **kw):
        trace = kw['trace']
        request = kw['request']
        try:
            return await table_select(
                cluster, table, 
                data=data, 
                method=request.method, **kw)
        except Exception as e:
            server.http_exception(500, trace.exception("error in cluster table select"))

    @server.api_route('/cluster/{cluster}/tables', methods=['GET'])
    async def cluster_tables_config_api(cluster, request: Request):
        return await cluster_tables_config(cluster,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_tables_config(cluster, **kw):
        return await tables_config(cluster, **kw)

    @server.trace
    async def tables_config(cluster, **kw):
        tables = await server.clusters.tables.select('name', where={'cluster': cluster})
        tables_config = {}
        for table in tables:
            config = table_config(cluster, table['name'], **kw)
            tables_config.update(config)
        return tables_config 

    @server.api_route('/cluster/{cluster}/table/{table}/config', methods=['GET'])
    async def cluster_table_config_api(cluster: str, table: str, request: Request):
        return await cluster_table_copy_auth(cluster, table,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_copy_auth(cluster, table, **kw):
        return await cluster_table_copy(cluster, table, **kw)

    @server.trace
    async def cluster_table_copy(cluster, table, **kw):
        """
        returns a select('*') of a loaded table & the tables 
        last_txn_time
        """
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']
        copy_only = False if not 'copy_only' in kw else kw['copy_only']


        endpoints = await get_table_endpoints(cluster, table, **kw)
        
        endpoints_info = await get_table_info(cluster, table, endpoints, **kw)
        endpoint_choice = random.choice(
            [e for e in endpoints['loaded']]
        )
        endpoint_info = endpoints_info['endpoints'][f"{endpoint_choice}_{table}"]
        path = endpoint_info['path']
        token = endpoint_info['token']

        #TODO - table_copy  / select should use server.data[] if querying local endpoint
        # i.e check if endpoint_info['uuid'] == node_id

        if endpoint_info['uuid'] == node_id:
            if not copy_only:
                table_copy = await server.get_table_copy(
                    endpoint_info['db_name'],
                    table
                )
            else:
                table_copy = await server.actions['select'](
                    endpoint_info['db_name'], 
                    table, 
                    **kw
                )
        else:
            # log clusters do not need last_txn_time via copy
            op = 'copy' if not copy_only else 'select' ## 

            # pull table copy & last_txn_time
            table_copy, rc = await probe(
                f"{path}/{op}",
                method='GET',
                token=token,
                session=await get_endpoint_sessions(
                    endpoint_info['uuid'],
                    **kw
                )
            )
        if not copy_only:
            return table_copy
        
        trace(f"log cluster - table_copy - {table_copy}")
        table_copy = table_copy['data'] if 'data' in table_copy else table_copy
        
        if cluster == pyql and table in ('state', 'tables', 'jobs'):
            return {
                'table_copy': table_copy, 
                'last_txn_time': time.time()
                }

        # log cluster
        last_txn_time = table_copy[-1]['timestamp'] if len(table_copy) > 0 else time.time()
        return {
            'table_copy': table_copy, 
            'last_txn_time': last_txn_time
            }
                
    @server.api_route('/cluster/{cluster}/table/{table}/config', methods=['GET'])
    async def cluster_table_config_api(cluster: str, table: str, request: Request):
        return await cluster_table_config_auth(cluster, table,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_config_auth(cluster, table, **kw):
        return await cluster_table_config(cluster, table, **kw)

    @server.trace
    async def cluster_table_config(cluster, table, **kw):
        return await endpoint_probe(cluster, table, method='GET', path=f'/config', **kw)

    
    @server.api_route('/cluster/{cluster}/table/{table}/update', methods=['POST'])
    async def cluster_table_update_api(cluster: str, table: str, request: Request, data: dict = None):
        return await cluster_table_update(cluster, table, data=data,  request=await server.process_request(request))
    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_update(cluster, table, data=None, **kw):
        return await table_update(cluster=cluster, table=table, data=data, **kw)

    @server.trace
    async def table_update(cluster=None, table=None, data=None, config=None, **kw):
        trace = kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        if not config == None: # this was invoked by a job
            cluster, table, data = config['cluster'], config['table'], config['data']
        return await cluster_table_change(
            cluster, table,'update', data, **kw)
    server.cluster_table_update = table_update
    server.clusterjobs['table_update'] = table_update
            
    @server.api_route('/cluster/{cluster}/table/{table}/insert', methods=['POST'])
    async def cluster_table_insert_api(cluster: str, table: str, data: dict, request: Request):
        return await cluster_table_insert(cluster, table, data,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_insert(cluster, table, data, **kw):
        return await table_insert(cluster, table, data, **kw)
    @server.trace
    async def table_insert(cluster, table, data, **kw):
        return await cluster_table_change(cluster, table, 'insert',  data, **kw)
    server.cluster_table_insert = table_insert

    @server.api_route('/cluster/{cluster}/table/{table}/delete', methods=['POST'])
    async def cluster_table_delete_api(cluster: str, table: str, data: dict, request: Request):
        return await cluster_table_delete(cluster, table, data,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_delete(cluster, table, data, **kw):
        return await table_delete(cluster, table, data, **kw)

    @server.trace
    async def table_delete(cluster, table, data, **kw):
        return await cluster_table_change(cluster, table, 'delete', data, **kw)

    @server.api_route('/cluster/{cluster}/table/{table}/{key}', methods=['GET', 'POST', 'DELETE'])
    async def cluster_table_key(cluster: str, table: str, key: str, request: Request, data: dict = None):
        return await cluster_table_key(cluster, table, key, data=data,  request=await server.process_request(request))
    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_key(cluster, table, key, **kw):
        trace = kw['trace']
        request = kw['request']
        if request.method == 'GET':
            return await endpoint_probe(cluster, table, path=f'/{key}', **kw)
        data = None
        if request.method == 'POST':
            try:
                data = kw['data']
            except Exception as e:
                server.http_exception(400, trace.error("expected json input for request"))
        primary = await server.clusters.tables.select(
            'config',
            where={'cluster': cluster, 'name': table})
        primary = primary[0]['config'][table]['primary_key']
        if request.method == 'POST':
            kw['data'] = {'set': data, 'where': {primary: key}}
            return await table_update(cluster=cluster, table=table, **kw)
        if request.method == 'DELETE':
            return await table_delete(cluster, table, {'where': {primary: key}}, **kw)




    @server.api_route('/cluster/{cluster}/table/{table}/pause/{pause}', methods=['POST'])
    async def cluster_table_pause_api(cluster: str, table: str, pause: str, request: Request):
        return await cluster_table_pause(cluster, table, pause,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_table_pause(cluster, table, pause, **kw):
        return await table_pause(cluster, table, pause, trace=kw['trace'])

    @server.trace
    async def table_pause(cluster, table, pause, **kw):
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        pause = True if pause == 'start' else False
        pause_set = {
            'set': {'is_paused': pause},
            'where': {'id': f"{cluster}_{table}"}
        }

        # overrides table paused checks
        kw['force'] = True

        result = await cluster_table_change(pyql, 'tables', 'update', pause_set, **kw)
        if 'delay_after_pause' in kw:
            await asyncio.sleep(kw['delay_after_pause'])
        trace.warning(f'cluster_table_pause {cluster} {table} pause {pause} result: {result}')
        return result
        
    @server.trace
    async def table_endpoint(cluster, table, endpoint, config=None, **kw):
        """ Sets state 
        cluster: uuid, table: name, endpoint: uuid, 
        config: {'in_sync': True|False, 'state': 'loaded|new'}
        last_mod_time: float(time.time())
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        set_config = config
        valid_inputs = ['in_sync', 'state']
        for cfg in set_config:
            if not cfg in valid_inputs:
                server.http_exception(
                    400, 
                    trace.error(f"invalid input {cfg}, supported config inputs {valid_inputs}")
                    )
        sync_set = {
            'set': set_config,
            'where': {'cluster': cluster, 'table_name': table, 'uuid': endpoint}
        }
        result = await cluster_table_change(pyql, 'state', 'update', sync_set, **kw)
        trace.warning(f'cluster_table_endpoint_sync {cluster} {table} endpoint {endpoint} result: {result}')
        return result

    @server.api_route('/cluster/{cluster}/tablelogs/{table}/{endpoint}/{action}', methods=['GET','POST'])
    async def cluster_table_endpoint_logs(
        cluster: str, 
        table: str, 
        endpoint: str, 
        action: str, 
        request: Request, 
        data: dict = None):
        return await cluster_table_endpoint_logs(cluster, table, endpoint, action, data=data,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_table_endpoint_logs(cluster, table, endpoint, action, **kw):
        request = kw['request']
        if request.method == 'GET':
            result, rc = await table_endpoint_logs(cluster, table, endpoint, action, **kw)
        if request.method == 'POST' and action == 'commit':
            result, rc = await commit_table_endpoint_logs(cluster, table, endpoint, txns=data, **kw)
        if not rc == 200:
            server.http_exception(rc, result)
        return {"message": result}

    @server.trace
    async def table_endpoint_logs(cluster, table, endpoint, action, **kw):
        """
        requires cluster uuid for cluster
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        cluster_table_endpoint_txns = {
            'select': None,
            'where': {
                'endpoint': endpoint,
                'cluster': cluster,
                'table_name': table
                }
        }
        if action == 'count':
            cluster_table_endpoint_txns['select'] = ['uuid']

        if action == 'getAll':
            cluster_table_endpoint_txns['select'] = ['*']
        response = await table_select(
            pyql, 
            'transactions',
            data=cluster_table_endpoint_txns,
            method='POST',
            **kw
            )
        if not response:
            return log.error("error pulling endpoint logs"), 500
        if action == 'count':
            log.warning(f"# count completed")
            return {"available_txns": len(response['data'])}, 200
        elif action == 'getAll':
            log.warning(f"# getAll completed")
            return response, 200
        else:
            server.http_exception(
                400,
                trace(f"get_cluster_table_endpoint_logs - invalid action provided")
            )

    @server.trace
    async def commit_table_endpoint_logs(cluster, table, endpoint, txns=None, **kw):
        """
            expects input 
            {'txns': ['uuid1', 'uuid2', 'uuid3']}
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        for txn in txns['txns']:
            delete_txn = {
                'where': {
                    'endpoint': endpoint,
                    'cluster': cluster,
                    'table_name': table,
                    'uuid': txn
                }
            }
            resp = await cluster_table_change(pyql, 'transactions', 'delete', delete_txn, **kw)
        return trace(f"successfully commited txns {txns}"), 200

    @server.trace
    async def get_txn_cluster_to_join(**kw):
        """
        Requirements:
        - limit of 3 pyql nodes per 1 txn cluster
        - when limit is reached, create new txn cluster if expanding
        """
        trace = kw['trace']
        trace(f"starting")

        txn_clusters = await server.clusters.clusters.select(
            '*',
            where={
                'type': 'log'
            }
        )
        data_and_txn_clusters = await server.clusters.data_to_txn_cluster.select('*')
        # need to iterate through server.clusters.custers type=log tables
        # and build data_and_txn_clusters_count based on this when deciding txn
        # cluster to join

        data_and_txn_clusters_count = Counter()
        for cluster_map in data_and_txn_clusters:
            txn_cluster_id = cluster_map['txn_cluster_id']
            if not txn_cluster_id in data_and_txn_clusters_count:
                data_and_txn_clusters_count[txn_cluster_id] = 0
            data_and_txn_clusters_count[txn_cluster_id] +=1
        # add all txn clusterss with 0 members
        for txn_cluster in txn_clusters:
            if not txn_cluster['id'] in data_and_txn_clusters_count:
                data_and_txn_clusters_count[txn_cluster['id']] = 0

        cluster_id = min(data_and_txn_clusters_count)
        
        for cluster_map in data_and_txn_clusters:
            if cluster_map['txn_cluster_id'] == cluster_id:
                trace(f"finished - txn_cluster: {cluster_map}")
                return cluster_map

    @server.trace
    async def join_cluster_pyql_bootstrap(config, **kw):
        trace = kw['trace']

        pyql = str(uuid.uuid1())
        pyql_txns_id = str(uuid.uuid1())

        # Boostrap initial txn log cluster
        txn_cluster_data = {
            "name": os.environ['HOSTNAME'],
            "path": f"{os.environ['PYQL_NODE']}:{os.environ['PYQL_PORT']}",
            "token": await server.env['PYQL_LOCAL_SERVICE_TOKEN'],
            "database": {
                'name': "transactions",
                'uuid': f"{node_id}"
            },
            "tables": [
                await server.get_table_config('transactions', 'txn_cluster_tables')
            ],
            "consistency": [] 
        }

        # Bootstrap pyql cluster
        await bootstrap_cluster(pyql, 'pyql', config, **kw)


        # For each table in bootstrap, create corresponding txn table
        for table in config['tables']:
            for table_name, _ in table.items():
                if table_name in ['state', 'tables', 'jobs']:
                    continue
                await server.create_txn_cluster_table(pyql, table_name)
                pyql_id_underscored = (
                    '_'.join(pyql.split('-'))
                )
                txn_cluster_data['tables'].append(
                    await server.get_table_config(
                        'transactions', 
                        f'txn_{pyql_id_underscored}_{table_name}'
                    )
                )
        # Bootstrap txn cluster
        await bootstrap_cluster(pyql_txns_id, f"{pyql_txns_id}_log", txn_cluster_data, **kw)
        
        # after bootstrap - assign auth to service id 

        service_id = await server.clusters.auth.select(
            'id', where={'parent': kw['authentication']})
        kw['authentication'] = service_id[0]['id']

        # Register Bootstrapped node in data_to_txn_cluster

        await server.clusters.data_to_txn_cluster.insert(
            **{
                'data_cluster_id': pyql,
                'txn_cluster_id': pyql_txns_id,
                'txn_cluster_name': pyql_txns_id
            }
        )


    @server.trace
    async def pyql_create_txn_cluster(config, txn_clusters, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']

        # need to create a new txn cluster
        admin_id = await server.clusters.auth.select('id', where={'username': 'admin'})
        service_id = kw['authentication']
        new_txn_cluster = str(uuid.uuid1())
        data = {
            'id': new_txn_cluster,
            'name': f"{new_txn_cluster}_log",
            'owner': service_id,
            'access': {'allow': [admin_id[0]['id'], service_id]},
            'type': 'log',
            'key': None,
            'created_by_endpoint': config['name'],
            'create_date': f'{datetime.now().date()}'
            }
        await cluster_table_change(pyql, 'clusters', 'insert', data, **kw)

        # add new endpoint to new cluster

        data = {
            'id': str(uuid.uuid1()),
            'uuid': config['database']['uuid'],
            'db_name': 'transactions',
            'path': config['path'],
            'token': config['token'],
            'cluster': new_txn_cluster
        }
        await cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)

        # add two random existing endpoints to new cluster
        for _ in range(2):
            existing = random.choice(txn_clusters)
            data = {
                'id': str(uuid.uuid1()),
                'uuid': existing['endpoints.uuid'],
                'db_name': 'transactions',
                'path': existing['endpoints.path'],
                'token': existing['endpoints.token'],
                'cluster': new_txn_cluster
            }
            await cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)

    @server.trace
    async def pyql_join_txn_cluster(config: dict, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']

        # check if can join any current txn cluster 
        # or create new cluster - limit 3 pyql endpoints
        # per txn cluster
        txn_clusters = await server.clusters.clusters.select(
            'clusters.id',
            'clusters.name',
            'endpoints.uuid',
            'endpoints.token',
            'endpoints.path',
            join={
                'endpoints': {
                    'clusters.id': 'endpoints.cluster'
                }
            },
            where={
                'clusters.type': 'log'
            }
        )
        log_clusters_and_endpoints = {}
        for cluster in txn_clusters:
            if not cluster['clusters.id'] in log_clusters_and_endpoints:
                log_clusters_and_endpoints[cluster['clusters.id']] = []
            log_clusters_and_endpoints[cluster['clusters.id']].append(
                cluster['endpoints.uuid']
            )
        joined_existing = False
        for cluster in log_clusters_and_endpoints:
            if len(log_clusters_and_endpoints[cluster]) < 3:
                # this node can join this cluster
                data = {
                    'id': str(uuid.uuid1()),
                    'uuid': config['database']['uuid'],
                    'db_name': 'transactions',
                    'path': config['path'],
                    'token': config['token'],
                    'cluster': cluster
                }
                await cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)

                # need to create state entries for new endpoints
                # for each table in log_cluster - create state entry for new endpoint
                tables = await server.clusters.tables.select('name', where={'cluster': cluster})
                tables = [table['name'] for table in tables]
                for table in tables:
                    data = {
                        'name': f"{config['database']['uuid']}_{table}",
                        'state': 'new',
                        'table_name': table,
                        'cluster': cluster,
                        'uuid': config['database']['uuid'], # used for syncing logs
                        'last_mod_time': 0.0
                    }
                    await cluster_table_change(pyql, 'state', 'insert', data, **kw)



                joined_existing = True
        if not joined_existing:
            await pyql_create_txn_cluster(
                config,
                txn_clusters,
                **kw
            )

    @server.trace
    async def join_cluster_create(cluster_name, config, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        # Creating a New Cluster
        cluster_id = str(uuid.uuid1())
        trace(f"creating new cluster with id {cluster_id}")

        data = {
            'id': cluster_id,
            'name': cluster_name,
            'owner': kw['authentication'], # added via @server.is_autenticated
            'access': {'allow': [kw['authentication']]},
            'type': 'data',
            'created_by_endpoint': config['name'],
            'create_date': f'{datetime.now().date()}'
            }
        create_result = await cluster_table_change(pyql, 'clusters', 'insert', data, **kw)
        trace(f"create cluster result: {create_result}")

        # Adding new Cluster to data_to_txn_cluster
        # ('data_cluster_id', str, 'UNIQUE'),
        # ('txn_cluster_id', str)
        txn_cluster = await get_txn_cluster_to_join(trace=trace)
        trace(f"txn cluster to join: {txn_cluster}")

        new_cluster_to_txn_map = {
            'data_cluster_id': cluster_id,
            'txn_cluster_name': txn_cluster['txn_cluster_name'],
            'txn_cluster_id': txn_cluster['txn_cluster_id'] # await get_txn_cluster_to_join()
        }
        join_txn_cluster_result = await cluster_table_change(
            pyql, 
            'data_to_txn_cluster', 
            'insert', 
            new_cluster_to_txn_map, 
            **kw
        )
        trace(f"join txn cluster - result: {join_txn_cluster_result}")
        return trace(f"completed")

    @server.trace
    async def join_cluster_create_or_update_endpoint(cluster_id, config, **kw):
        """
        called by join_cluster to create new cluster endpoint
        or update the config of an existing 
        """
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        trace(f"started for cluster: {cluster_id}")

        endpoints = await server.clusters.endpoints.select(
            'uuid', 
            where={'cluster': cluster_id}
        )
        new_endpoint_or_database = False
        if not config['database']['uuid'] in [ endpoint['uuid'] for endpoint in endpoints ]:
            #add endpoint
            new_endpoint_or_database = True
            new_endpoint_id = str(uuid.uuid1())
            data = {
                'id': str(uuid.uuid1()),
                'uuid': config['database']['uuid'],
                'db_name': config['database']['name'],
                'path': config['path'],
                'token': config['token'],
                'cluster': cluster_id
            }

            trace(f"adding new endpoint with id: {new_endpoint_id} {config['database']}")

            await cluster_table_change(pyql, 'endpoints', 'insert', data, **kw)

        else:
            #update endpoint latest path info - if different
            trace(
                f"endpoint with id {config['database']['uuid']} already exists in cluster {cluster_id} endpoints {endpoints}"
                )
            update_set = {
                'set': {
                    'path': config['path'], 
                    'token': config['token']},
                'where': {
                    'uuid': config['database']['uuid']
                }
            }
            if len(endpoints) == 1 and cluster_id == pyql:
                #Single node pyql cluster - path changed
                await server.clusters.endpoints.update(
                    **update_set['set'],
                    where=update_set['where']
                )
            else:
                await cluster_table_change(pyql, 'endpoints', 'update', update_set, **kw)
                if cluster_id == await server.env['PYQL_UUID']:
                    trace(f"existing endpoint detected for multi-node cluster, marking new endpoint tables 'stale'")
                    await cluster_table_change(
                        pyql, 'state', 'update', 
                        {
                            'set': {
                                'state': 'stale',
                                'info': {
                                    'stale reason': 'existing endpoint rejoined cluster',
                                    'operation': trace.get_root_operation(),
                                    'node': node_id
                                }
                            }, 
                            'where': {
                                'uuid': config['database']['uuid']
                                }
                        }, 
                        **kw
                    )
        trace.warning(f"completed - new_endpoint_or_database: {new_endpoint_or_database}")
        return new_endpoint_or_database
    @server.trace
    async def join_cluster_create_tables(cluster_id: str, config: dict, **kw) -> list:
        """
        creates new tables if not created already and returns
        list of created tables
        """
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        trace(f"started for cluster: {cluster_id}")

        tables = await server.clusters.tables.select('name', where={'cluster': cluster_id})
        tables = [table['name'] for table in tables]

        txn_cluster_id = await server.clusters.data_to_txn_cluster.select(
            'txn_cluster_id',
            where={
                'data_cluster_id': cluster_id
            }
        )
        txn_cluster_id = txn_cluster_id[0]['txn_cluster_id']

        # if tables not exist, add
        new_tables = []
        for table in config['tables']:
            for table_name, tb_config in table.items():
                if not table_name in tables:
                    new_tables.append(table_name)
                    #JobIfy - create as job so config
                    data = {
                        'id': f"{cluster_id}_{table_name}",
                        'name': table_name,
                        'cluster': cluster_id,
                        'config': tb_config,
                        'consistency': table_name in config['consistency'],
                        'is_paused': False
                    }
                    await cluster_table_change(pyql, 'tables', 'insert', data, **kw)
                    if not (cluster_id == pyql and table_name in ['jobs', 'state', 'tables']):
                        cluster_id_under = '_'.join(cluster_id.split('-'))
                        await cluster_txn_table_create(
                            txn_cluster_id,
                            f'{cluster_id_under}_{table_name}',
                            **kw
                        )
        trace(f"finished with {new_tables} new tables in cluster")
        return new_tables
    
    @server.trace
    async def join_cluster_update_state(cluster_id, new_tables, config, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']

        trace(f"started for cluster: {cluster_id} and new_tables: {new_tables}")

        tables = await server.clusters.tables.select('name', where={'cluster': cluster_id})
        tables = [table['name'] for table in tables]
        endpoints = await server.clusters.endpoints.select('*', where={'cluster': cluster_id})    
        state = await server.clusters.state.select('name', where={'cluster': cluster_id})
        state = [tbEp['name'] for tbEp in state]

        for table in tables:
            for endpoint in endpoints:
                table_endpoint = f"{endpoint['uuid']}_{table}"
                if not table_endpoint in state:
                    # check if this table was added along with endpoint, and does not need to be created 
                    load_state = 'loaded' if endpoint['uuid'] == config['database']['uuid'] else 'new'
                    if not table in new_tables:
                        #Table arleady existed in cluster, but not in endpoint with same table was added
                        sync_state = False
                        load_state = 'new'
                    else:
                        # New tables in a cluster are automatically marked in sync by endpoint which added
                        sync_state = True
                    # Get DB Name to update
                    data = {
                        'name': table_endpoint,
                        'state': load_state,
                        'table_name': table,
                        'cluster': cluster_id,
                        'uuid': endpoint['uuid'], # used for syncing logs
                        'last_mod_time': 0.0
                    }
                    trace(f"adding new table endpoint: {data}")
                    await cluster_table_change(pyql, 'state', 'insert', data, **kw)
        trace(f"finished")

    @server.trace
    async def join_cluster_pyql_finish_setup(
        cluster_id: str, 
        is_pyql_bootstrapped: bool,
        is_new_endpoint: bool,
        config: dict, 
        **kw):
        """
        used to perform final setup of a new or rejoining pyql node 
        """
        trace = kw['trace']

        trace(f"starting with is_pyql_bootstrapped={is_pyql_bootstrapped}, is_new_endpoint={is_new_endpoint}")

        if is_pyql_bootstrapped and not is_new_endpoint:
            trace(f"Not bootrapping cluster, not a new endpoint, starting tablesync_mgr")
            await tablesync_mgr(trace=trace)
        # pyql setup - sets pyql_uuid in env 
        setup, rc = await probe(
            f"http://{config['path']}/pyql/setup",
            method='POST',
            data={'PYQL_UUID': cluster_id},
            token=config['token'],
            session=await get_endpoint_sessions(config['database']['uuid'], **kw),
            **kw
        )
        trace(f"pyql setup result - {setup} {rc}")
        # auth setup - applys cluster service token in joining pyql node, and pulls key
        result, rc = await probe(
            f"http://{config['path']}/auth/setup/cluster",
            method='POST',
            data={
                'PYQL_CLUSTER_SERVICE_TOKEN': await server.env['PYQL_CLUSTER_SERVICE_TOKEN']
            },
            token=config['token'],
            session=await get_endpoint_sessions(config['database']['uuid'], **kw),
            **kw
        )
        trace.warning(f"completed auth setup for new pyql endpoint: result {result} {rc}")
        # Trigger quorum update
        await cluster_quorum_check(trace=trace)
        return trace("completed")
        

    @server.api_route('/cluster/{cluster_name}/join', methods=['POST'])
    async def join_cluster_api(cluster_name: str, config: dict, request: Request):
        return await join_cluster_auth(cluster_name, config,  request=await server.process_request(request))

    @server.is_authenticated('cluster')
    @server.trace
    async def join_cluster_auth(cluster_name, config, **kw):
        return await join_cluster(cluster_name, config, **kw)

    @server.trace
    async def join_cluster(cluster_name, config, **kw):
        trace=kw['trace']
        kw['loop'] = asyncio.get_running_loop()
        request = kw['request'] if 'request' in kw else None
    
        db = server.data['cluster']
        new_endpoint_or_database = False
        is_pyql_bootstrapped = False
        pyql = None

        trace.info(f"join cluster for {cluster_name} with kwargs {kw}")

        # check if pyql is bootstrapped 
        clusters = await server.clusters.clusters.select(
                '*', where={'name': 'pyql'})
        for cluster in clusters:
            if cluster['name'] == 'pyql':
                is_pyql_bootstrapped, pyql = True, cluster['id']

        if is_pyql_bootstrapped and os.environ['PYQL_CLUSTER_ACTION'] == 'init':
            if config['database']['uuid'] == node_id:
                return {"message": trace("pyql cluster already initialized")}

        if not 'authentication' in kw:
            admin_id = await server.clusters.auth.select('id', where={'username': 'admin'})
            kw['authentication'] = admin_id[0]['id']

        if not is_pyql_bootstrapped and cluster_name == 'pyql':
            await join_cluster_pyql_bootstrap(
                config, **kw
            )
            service_id = await server.clusters.auth.select(
                'id', 
                where={'parent': kw['authentication']})
            service_id = service_id[0]['id']
            kw['authentication'] = service_id

        else: 
            """Pyql Cluster was already Bootstrapped"""
            if cluster_name == 'pyql':
                await pyql_join_txn_cluster(config, **kw)
                await asyncio.sleep(3)
 
        clusters = await server.clusters.clusters.select(
            '*', where={'owner': kw['authentication']})

        for cluster in clusters:
            if cluster['name'] == 'pyql':
                await server.env.set_item('PYQL_UUID', cluster['id'])

        if not cluster_name in [cluster['name'] for cluster in clusters]:
            await join_cluster_create(cluster_name, config, **kw)
            await asyncio.sleep(3)
            
        cluster_id = await server.clusters.clusters.select(
            '*', where={
                    'owner': kw['authentication'], 
                    'name': cluster_name
                })
        cluster_id = cluster_id[0]['id']

        #check for existing endpoint in cluster: cluster_id 
        new_endpoint_or_database = await join_cluster_create_or_update_endpoint(
            cluster_id, config, **kw
        )
        await asyncio.sleep(10)

        # check for exiting tables in cluster 
        new_tables = await join_cluster_create_tables(cluster_id, config, **kw)
        await asyncio.sleep(10)

        if new_endpoint_or_database == True:
            trace(f"new endpoint or database detected, need to join_cluster_update_state")
            await join_cluster_update_state(cluster_id, new_tables, config, **kw)
            await asyncio.sleep(10)

        if cluster_name == 'pyql':
            await join_cluster_pyql_finish_setup(
                cluster_id,
                is_pyql_bootstrapped,
                new_endpoint_or_database,
                config,
                **kw,
            )
        return {"message": trace.warning(f"join cluster {cluster_name} for endpoint {config['name']} completed successfully")}, 200
    server.join_cluster = join_cluster

    @server.trace
    async def re_queue_job(job, **kw):
        await job_update(job['type'], job['id'],'queued', {"message": "job was requeued"}, **kw)

    @server.api_route('/cluster/pyql/jobmgr/cleanup', methods=['POST'])
    async def cluster_jobmgr_cleanup_api(request: Request):
        return await cluster_jobmgr_cleanup( request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_jobmgr_cleanup(**kw):
        return await jobmgr_cleanup(**kw)
    @server.trace
    async def jobmgr_cleanup(**kw):
        """
            invoked on-demand or by cron to check for stale jobs & requeue
        """ 
        trace=kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        jobs = await table_select(pyql, 'jobs', method='GET', **kw)
        jobs = jobs['data']
        for job in jobs:
            if not job['next_run_time'] == None:
                # Cron Jobs 
                if time.time() - float(job['next_run_time']) > 240.0:
                    if not job['node'] == None:
                        await re_queue_job(job, trace=kw['trace'])
                        continue
                    else:
                        trace.error(f"job {job['id']} next_run_time is set but stuck for an un-known reason")
            if not job['start_time'] == None:
                time_running = time.time() - float(job['start_time'])
                if time_running > 240.0:
                    # job has been running for more than 4 minutes
                    trace.warning(f"job {job['id']} has been {job['status']} for more than {time_running} seconds - requeuing")
                    await re_queue_job(job, trace=kw['trace'])
                if job['status'] == 'queued':
                    if time_running > 30.0:
                        trace.warning(f"job {job['id']} has been queued for more {time_running} seconds - requeuing")
                        await re_queue_job(job, trace=kw['trace'])
            else:
                if job['status'] == 'queued':
                    # add start_time to check if job is stuck
                    await cluster_table_change(
                        pyql, 'jobs', 'update', 
                        {'set': {
                            'start_time': time.time()}, 
                        'where': {
                            'id': job['id']}}, 
                        **kw)
            if job['status'] == 'waiting':
                waiting_on = None
                for jb in jobs:
                    if 'nextJob' in jb['config']:
                        if jb['config']['nextJob'] == job['id']:
                            waiting_on = jb['id']
                            break
                if waiting_on == None:
                    trace.warning(f"Job {job['name']} was waiting on another job which did not correctly queue, queuing now.")
                    await re_queue_job(job, **kw)
                    
        return {"message": trace.warning(f"job manager cleanup completed")}
    server.clusterjobs['jobmgr_cleanup'] = jobmgr_cleanup

    @server.api_route('/cluster/jobqueue/{job_type}', methods=['POST'])
    async def cluster_jobqueue(job_type: str, request: Request):
        return await cluster_jobqueue(job_type,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_jobqueue(job_type, **kw):
        return await jobqueue(job_type, **kw)
    

    @server.trace
    async def jobqueue_reserve_job(job, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']
        reservation = trace.get_root_operation()

        async def reserve_or_rollback(rollback=False):

            job_update = {
                'set': {
                    'node': node_id if not rollback else None,
                    'reservation': reservation if not rollback else None
                }, 
                'where': {
                    'id': job['id'], 
                    'node': None if not rollback else node_id
                }
            }
            if rollback:
                job_update['reservation'] = reservation

            result = await cluster_table_change(pyql, 'jobs', 'update', job_update, **kw)
            op = 'reserve' if not rollback else 'rollback'
            return trace(f"{op} completed for job {job['id']} with result {result}")
        _ = await reserve_or_rollback()

        # verify if job was reserved by node and pull config
        job_select = {
            'select': ['*'],
            'where': {
                'id': job['id']
            }
        }
        start_time, max_timeout = time.time(), 20.0
        while time.time() - start_time < max_timeout:
            job_check = await table_select(pyql, 'jobs', data=job_select, method='POST', **kw)
            if len(job_check['data']) == 0:
                return {"message": trace(f"failed to reserve job {job['id']}, no longer exists")}
            trace(f"job_check: {job_check}")
            job_check = job_check['data'][0]
            if job_check['node'] == None:
                # wait until 'node' is assigned
                await asyncio.sleep(0.1)
                continue
            if (job_check['node'] == node_id and 
                job_check['reservation'] == reservation ):
                return job_check
            await reserve_or_rollback(rollback=True)
            return {"message": trace(f"{job['id']} was reserved by another worker")}
        else:
            _ = await reserve_or_rollback(rollback=True)
            return {"message": trace.error(f"timeout of {max_timeout} reached while trying to reserve job")}

    @server.trace
    async def jobqueue(job_type, node=None, **kw):
        """
            Used by jobworkers or tablesyncers to pull jobs from clusters job queues
            job_type = 'job|syncjob|cron'
        """
        trace = kw['trace']
        queue = f'{job_type}s' if not job_type == 'cron' else job_type

        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        ready = await server.clusters.quorum.select('ready', where={'node': node_id})
        ready  = ready[0]['ready']
        if pyql == None or not ready:
            return {"message": trace("cluster is not ready or still bootstrapping, try again later")}

        endpoints = await server.clusters.endpoints.select(
            '*'
        )

        if len(endpoints) == 0:
            return {"message": trace("cluster is bootstrapped, but still syncing")}

        quorum = await cluster_quorum_query()

        node = node_id

        trace(f"checking for {job_type} jobs to run")

        job_select = {
            'select': ['id', 'name', 'type', 'next_run_time', 'node'], 
            'where':{
                'status': 'queued',
                'type': queue
            }
        }
        if not job_type == 'cron':
            job_select['where']['node'] = None
        trace("starting to pull list of jobs")
        job_list = await table_select(
            pyql, 'jobs', data=job_select, method='POST', quorum=quorum, **kw)
        if not job_list:
            return {"message": trace("unable to pull jobs at this time")}
        trace(f"type {job_type} - finished pulling list of jobs - job_list {job_list} ")
        job_list = job_list['data']
        for i, job in enumerate(job_list):
            if not job['next_run_time'] == None:
                #Removes queued job from list if next_run_time is still in future 
                if float(job['next_run_time']) < time.time():
                    job_list.pop(i)
                if not job['node'] == None:
                    if time.time() - float(job['next_run_time']) > 120.0:
                        trace.error(f"found stuck job {job['id']} assigned to node {job['node']} - begin re_queue job")
                        await re_queue_job(job, trace=kw['trace'])
                        trace(f"found stuck job assigned to node {job['node']} - finished re_queue job")

        if len(job_list) == 0:
            return {"message": trace("no jobs to process at this time")}
        if job_type == 'cron':
            job_list = sorted(job_list, key=lambda job: job['next_run_time'])
            job = job_list[0]
        else:
            latest = 3 if len(job_list) >= 3 else len(job_list)
            job_index = randrange(latest-1) if latest -1 > 0 else 0
            job = job_list[job_index]
        
        job_select['where']['id'] = job['id']

        trace.warning(f"Attempt to reserve job {job} if no other node has taken ")

        # reserve job
        return await jobqueue_reserve_job(job, **kw)

    @server.api_route('/cluster/job/{job_type}/{job_id}/{status}', methods=['POST'])
    async def cluster_job_update_api(job_type: str, job_id: str, status: str, request: Request, job_info: dict = None):
        return await cluster_job_update(job_type, job_id, status, job_info=job_info,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_job_update(job_type, job_id, status, **kw):
        return await job_update(job_type, job_id, status, **kw)

    async def job_update(job_type, job_id, status, job_info={}, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        trace=kw['trace']
        if status == 'finished':
            update_from = {'where': {'id': job_id}}
            if job_type == 'cron':
                cron_select = {'select': ['id', 'config'], 'where': {'id': job_id}}
                job = await table_select(pyql, 'jobs', data=cron_select, method='POST', **kw)
                job = job['data'][0]
                update_from['set'] = {
                    'node': None, 
                    'status': 'queued',
                    'start_time': None}
                if job:
                    update_from['set']['next_run_time'] = str(time.time()+ job['config']['interval'])
                else:
                    update_from['set']['next_run_time'] = str(time.time() + 25.0)
                    trace.error(f"error pulling cron job {job_id} - proceeding to mark finished")
                return await cluster_table_change(pyql, 'jobs', 'update', update_from, **kw) 
            return await cluster_table_change(pyql, 'jobs', 'delete', update_from, **kw)
        if status == 'running' or status == 'queued':
            update_set = {'last_error': {}, 'status': status}
            for k,v in job_info.items():
                if k =='start_time' or k == 'status':
                    update_set[k] = v
                    continue
                update_set['last_error'][k] = v
            update_where = {'set': update_set, 'where': {'id': job_id}}
            if status =='queued':
                update_where['set']['node'] = None
                update_where['set']['start_time'] = None
                if job_type == 'cron':
                    update_where['set']['next_run_time'] = str(time.time())
            else:
                update_where['set']['start_time'] = str(time.time())
            return await cluster_table_change(pyql, 'jobs', 'update', update_where, **kw)

    @server.api_route('/cluster/{cluster}/table/{table}/recovery', methods=['POST'])
    async def cluster_table_sync_recovery(cluster: str, table: str, reqeust: Request): 
        return await cluster_table_sync_recovery(cluster, table,  request=await server.process_request(request))
    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_table_sync_recovery(cluster, table, **kw):
        """
        expects cluster uuid input for cluster, table string
        """
        return await table_sync_recovery(cluster, table, **kw)
    @server.trace
    async def table_sync_recovery(cluster, table, **kw):
        """
            run when all table-endpoints are in_sync=False
        """
        trace = kw['trace']
        #need to check quorum as all endpoints are currently in_sync = False for table
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        """
        if cluster == pyql:
            quorum_check, rc = cluster_quorum(trace=kw['trace'])
            if not quorum_check['quorum']['in_quorum'] == True:
                error = f"unable to perform while outOfQuorum - quorum {quorum_check}"
                return {"error": trace.error(error)}
        """
        quorum_check = kw['quorum']

        # Need to check all endpoints for the most up-to-date loaded table
        select = {'select': ['path', 'db_name', 'uuid'], 'where': {'cluster': cluster}}
        cluster_endpoints = await server.clusters.endpoints.select(
            'path', 'db_name', 'uuid', 'token',
            where={'cluster': cluster}
        )
        latest = {'endpoint': None, 'last_mod_time': 0.0}
        trace.warning(f"cluster {cluster} endpoints {cluster_endpoints}")
        find_latest = {'select': ['last_mod_time'], 'where': {'table_name': table}}
        for endpoint in cluster_endpoints:
            if cluster == pyql and not endpoint['uuid'] in quorum_check['quorum']['nodes']['nodes']:
                trace.warning(f"endpoint {endpoint} is not in quorum, so assumed as dead")
                continue
            db_name = endpoint['db_name'] if cluster == pyql else 'pyql'
            pyql_tb_check, rc = await probe(
                f"http://{endpoint['path']}/db/{db_name}/table/pyql/select",
                method='POST',
                token=endpoint['token'],
                data=find_latest,
                session=await get_endpoint_sessions(endpoint['uuid'], **kw),
                timeout=2.0,
                **kw
            )
            trace(f"table_sync_recovery - checking last_mod_time on cluster {cluster} endpoint {endpoint}")
            if len(pyql_tb_check) > 0 and pyql_tb_check['data'][0]['last_mod_time'] > latest['last_mod_time']:
                latest['endpoint'] = endpoint['uuid']
                latest['last_mod_time'] = pyql_tb_check['data'][0]['last_mod_time']
        trace(f"table_sync_recovery latest endpoint is {latest['endpoint']}")
        update_set_in_sync = {
            'set': {'in_sync': True}, 
            'where': {
                'name': f"{latest['endpoint']}{table}"
                }
            }
        if cluster == pyql and table == 'state':
            #special case - cannot update in_sync True via clusterSvcName - still no in_sync endpoints
            for endpoint in cluster_endpoints:
                if not endpoint['uuid'] in quorum_check['quorum']['nodes']['nodes']:
                    trace.warning(f"table_sync_recovery - endpoint {endpoint} is not in quorum, so assumed as dead")
                    continue
                stateUpdate, rc = await probe(
                    f"http://{endpoint['path']}/db/cluster/table/state/update",
                    'POST',
                    update_set_in_sync,
                    token=endpoint['token'],
                    session=await get_endpoint_sessions(endpoint['uuid'], **kw),
                    timeout=2.0,
                    **kw
                )
        else:
            await cluster_table_change(pyql, 'state', 'update', update_set_in_sync, **kw)
            #cluster_table_update(pyql, 'state', update_set_in_sync)
        trace.warning(f"table_sync_recovery completed selecting an endpoint as in_sync -  {latest['endpoint']} - need to requeue job and resync remaining nodes")
        return {"message": trace("table_sync_recovery completed")}

    @server.api_route('/cluster/pyql/tablesync/check', methods=['POST'])
    async def cluster_tablesync_mgr(request: Request):
        return await cluster_tablesync_mgr( request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_tablesync_mgr(**kw):
        return await tablesync_mgr(action, trace=kw['trace'])
    @server.trace
    async def tablesync_mgr(**kw):
        """
            invoked regularly by cron or ondemand 

            creates jobs which bring a stale / new table endpoint into 'loaded' state
        """
        trace=kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']

        trace(f"starting")

        jobs_to_create = {}
        jobs = {}
        tables = await server.clusters.tables.select('name', 'cluster')
        for table in tables:
            cluster = table['cluster']
            table_name = table['name']
            endpoints = await get_table_endpoints(cluster, table_name, **kw)
            if not len(endpoints['loaded'].keys()) > 0:
                trace.error(f"this should not happen, detected all endpoints for {cluster} {table_name} are not loaded")
                await table_sync_recovery(cluster, table_name, **kw)
                endpoints = await get_table_endpoints(cluster, table_name, **kw)

            new_or_stale_endpoints = endpoints['new']
            new_or_stale_endpoints.update(endpoints['stale'])
            trace(f"new_or_stale_endpoints detected: {new_or_stale_endpoints}")

            for endpoint in new_or_stale_endpoints:
                endpoint_path = new_or_stale_endpoints[endpoint]['path']
                if not cluster in jobs_to_create:
                    jobs_to_create[cluster] = {}
                if not table_name in jobs_to_create[table['cluster']]:
                    jobs_to_create[cluster][table_name] = []
                jobs_to_create[cluster][table_name].append({'endpoint': endpoint, 'path': endpoint_path})
        for cluster in jobs_to_create:
            jobs[cluster] = []
            for table in jobs_to_create[cluster]:
                # Add sync_table job for each table in cluster
                jobs[cluster].append({
                    'job': f'sync_table_{cluster}_{table}',
                    'job_type': 'syncjobs',
                    'action': 'table_sync_run',
                    'table': table, 
                    'table_paths': jobs_to_create[cluster][table],
                    'cluster': cluster,
                    'config': {
                        'cluster': cluster, 
                        'table': table, 
                        'job': f'sync_table_{cluster}_{table}'}
                    })
        
        for cluster in jobs:
            tables_in_jobs = [job['table'] for job in jobs[cluster]]
            order = [
                'data_to_txn_cluster',
                'state',
                'tables',
                'clusters', 
                'auth', 
                'endpoints', 
                'jobs'
            ]
            pyql_txn_tables = False
            for table in order:
                for job_table in tables_in_jobs:
                    if table in job_table:
                        pyql_txn_tables = True

            if cluster == pyql or pyql_txn_tables:
                #order = ['jobs', 'state', 'tables', 'clusters', 'auth', 'endpoints', 'transactions'] # 
                jobs_to_run_ordered = []
                ready_jobs = []
                while len(order) > 0:
                    #stateCheck = False
                    last_pop = None
                    for job in jobs[cluster]:
                        if len(order) == 0:
                            break
                        if order[0] in job['table']:
                            #if order[0] == 'state':
                            #    stateCheck = True
                            last_pop = order.pop(0)
                            jobs_to_run_ordered.append(job)
                            """
                            if last_pop == 'state':
                                for endpoint in job['tablePaths']:
                                    ready_jobs.append({
                                        'job': f"mark_ready_{endpoint['endpoint']}",
                                        'job_type': 'cluster',
                                        'action': 'update_cluster_ready',
                                        'config': {'ready': True, 'path': endpoint['path']}
                                    })
                            """
                    if last_pop == None:
                        order.pop(0)
                jobs_to_run_ordered = jobs_to_run_ordered + ready_jobs
                await wait_on_jobs(pyql, 0, jobs_to_run_ordered)
            else:
                for job in jobs[cluster]:
                    await jobs_add(job, trace=trace)
        trace.info(f"cluster_tablesync_mgr created {jobs} for outofSync endpoints")
        return {"jobs": jobs}
    server.clusterjobs['tablesync_mgr'] = tablesync_mgr

    
    @server.trace
    async def txn_table_sync(cluster, table, alive_endpoints, table_endpoints, **kw):
        """
        used to sync log type tables in cluster in following events:
        - new endpoint is added to pyql cluster and txn_cluster is not reached max endpoitns
        - existing endpoint re-joins pyql cluster - existing txn_cluster tables are stale
        - out of quorum endpoint now in quorum 
        1. Get dict of endpoints - stale / new
        2. check live-ness
        3. create table, if new
        4. Get Copy of a 'loaded' table
        5. Pause Ops to this table in Txn cluster
        6. Final Sync 
        7. Un-Pause & Resume ops
        """
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        loop = kw['loop']
        trace = kw['trace']

        pyql = await server.env['PYQL_UUID']
        pyql_under = '_'.join(pyql.split('-'))

        new_or_stale_endpoints = {}
        new_or_stale_endpoints.update(table_endpoints['new'])
        new_or_stale_endpoints.update(table_endpoints['stale'])

        trace(f"started - new_or_stale_endpoints: {new_or_stale_endpoints} - alive_endpoints: {alive_endpoints} ")

        ## create table 
        # create tables on new endpoints
        if len(table_endpoints['new']) > 0:
            table_config = None
            create_requests = {} 
            for _new_endpoint in table_endpoints['new']:
                if not _new_endpoint in alive_endpoints:
                    continue
                new_endpoint = table_endpoints['new'][_new_endpoint]
                
                # avoid pulling table config twice
                table_config = await cluster_table_config(
                    cluster, table, **kw
                    ) if table_config == None else table_config 
            
                # trigger table creation on new_endpoint
                db = new_endpoint['db_name']
                path = new_endpoint['path']
                epuuid = new_endpoint['uuid']
                token = new_endpoint['token']

                create_requests[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/{table}/create",
                    'data': table_config,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            create_table_results = await async_request_multi(
                create_requests, 
                'POST', 
                loop=loop
            )
            trace(f"{cluster} {table} create_table_results: {create_table_results}")

        if f'{pyql_under}_tables' in table:
            await table_pause(cluster, table, 'start', **kw)
            await asyncio.sleep(5)
        # get copy
        table_copy = await cluster_table_copy(cluster, table, copy_only=True, **kw)

        trace(f"{cluster} {table} - table_copy: - {table_copy}")

        if len(table_copy['table_copy']) > 0:
            # sync tables - pre cutover
            sync_requests = {}
            for _endpoint in new_or_stale_endpoints:
                if not _endpoint in alive_endpoints:
                    continue
                endpoint = new_or_stale_endpoints[_endpoint]
        
                db = endpoint['db_name']
                path = endpoint['path']
                epuuid = endpoint['uuid']
                token = endpoint['token']

                sync_requests[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/{table}/sync",
                    'data': table_copy,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            sync_table_results = await async_request_multi(
                sync_requests, 
                'POST', 
                loop=loop
            )
        else:
            sync_table_results = {}
            for _endpoint in new_or_stale_endpoints:
                if not _endpoint in alive_endpoints:
                    continue
                endpoint = new_or_stale_endpoints[_endpoint]
                sync_table_results[endpoint['uuid']] = {'status': 200}
        trace(f"sync_table_results: {sync_table_results}")

        # begin cut-over
        if not f'{pyql_under}_tables' in table:
            await table_pause(cluster, table, 'start', **kw)
            await asyncio.sleep(5)

        if len(table_copy['table_copy']) > 0:
            # pull changes 
            latest_timestamp = table_copy['table_copy'][-1]['timestamp']
            select_data = {
                'select': ['*'],
                'where': [
                    ['timestamp', '>', latest_timestamp]
                ]
            }
        else:
            select_data = None

        table_changes = await table_select(
            cluster,
            table,
            data=select_data,
            **kw
        )

        trace(f"{cluster} {table} - in-cutover - table_changes: - {table_changes}")

        while len(table_changes['data']) > 0:

            sync_changes_requests = {}
            for _endpoint in sync_table_results:
                if not sync_table_results[_endpoint]['status'] == 200:
                    continue
                endpoint = new_or_stale_endpoints[_endpoint]
        
                db = endpoint['db_name']
                path = endpoint['path']
                epuuid = endpoint['uuid']
                token = endpoint['token']

                sync_changes_requests[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/{table}/insert",
                    'data': {'params': table_changes['data']},
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            sync_changes_results = await async_request_multi(
                sync_requests, 
                'POST', 
                loop=loop
            )

            # check for new changes
            latest_timestamp = table_changes['data'][-1]['timestamp']
            select_data = {
                'select': ['*'],
                'where': [
                    ['timestamp', '>', latest_timestamp]
                ]
            }
            table_changes = await table_select(
                cluster,
                table,
                data=select_data,
                **kw
            )

            trace(f"{cluster} {table} - in-cutover - table_changes: - {table_changes}")

        else:
            sync_changes_results = {}
            for _endpoint in sync_table_results:
                endpoint = new_or_stale_endpoints[_endpoint]
                sync_changes_results[endpoint['uuid']] = {'status': 200}

        trace(f"sync_changes_results:  {sync_changes_results}")

        # mark endpoint loaded
        # mark table endpoint loaded
        state_update_results = []
        state_updates = []

        for endpoint in sync_changes_results:
            if not sync_changes_results[endpoint]['status'] == 200:
                continue
            if not f'{pyql_under}_state' in table:
                def get_state_change():
                    return cluster_table_change(
                        pyql,
                        'state',
                        'update',
                        {
                            'set': {
                                'state': 'loaded',
                                'info': {}
                                },
                            'where': {
                                'name': f"{endpoint}_{table}"
                                }
                        },
                        trace=trace,
                        force=True if f'{pyql_under}_tables' in table else False,
                        loop=loop
                    )
            else:
                async def get_state_change():
                    state_change = await cluster_table_change(
                        pyql,
                        'state',
                        'update',
                        {
                            'set': {
                                'state': 'loaded',
                                'info': {}
                                },
                            'where': {
                                'name': f"{endpoint}_{table}"
                                }
                        },
                        trace=trace,
                        force=True,
                        loop=loop
                    )
                    # check if this txn table is used by pyql state 
                    trace(f"pyql state txn table detected, waiting 5 sec then grabbing last txn table")
                    await asyncio.sleep(5)
                    state_endpoint = new_or_stale_endpoints[endpoint]
            
                    db = state_endpoint['db_name']
                    path = state_endpoint['path']
                    epuuid = state_endpoint['uuid']
                    token = state_endpoint['token']

                    latest_state_data = table_changes['data'] if len(table_changes['data']) > 0 else table_copy['table_copy']

                    latest_state_timestamp = latest_state_data[-1]['timestamp']
                    state_select_data = {
                        'select': ['*'],
                        'where': [
                            ['timestamp', '>', latest_state_timestamp]
                        ]
                    }
                    latest_state_changes = await table_select(
                        cluster,
                        table,
                        data=state_select_data,
                        exclude=epuuid, # Avoids use of this endpoint ( marked 'loaded' but not fully synced, yet) 
                        **kw
                    )
                    trace(f"lastest state change: {latest_state_changes}")

                    state_requests = {}
                    if len(latest_state_changes['data']) > 0:
                        state_requests[epuuid] = {
                            'path': f"http://{path}/db/{db}/table/{table}/insert",
                            'data': {'params': latest_state_changes['data']},
                            'timeout': 2.0,
                            'headers': await get_auth_http_headers('remote', token=token),
                            'session': await get_endpoint_sessions(epuuid, **kw)
                        }
                        state_change_result = await async_request_multi(
                            state_requests, 
                            'POST', 
                            loop=loop
                        )
                        trace(f"state_change_result: - {state_change_result}")
            state_update_results.append(
                await get_state_change()
            )
        if f'{pyql_under}_tables' in table:
            trace(f"pyql tables txn table detected, waiting 5 sec before un-pausing table")
            await asyncio.sleep(5)
        # end cut-over
        await table_pause(cluster, table, 'stop', **kw)

        return {
            "sync_table_results": sync_table_results,
            "sync_changes_results": sync_changes_results, 
            "state_updates": {
                "state_update_results": state_update_results
            }
        }

    @server.trace
    async def pyql_state_sync_run(table, alive_endpoints, table_endpoints, **kw):
        """
        to be used when syncing pyql table ['state', 'tables', 'jobs']
        as logs are not used for syncing
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID']

        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        loop = kw['loop']
        
        # table should have been created for new endpoints already

        # issue / start cutover
        pause_table = await table_pause(pyql, table, 'start', **kw)
        trace(f"pause_table starting - {pause_table}")
        await asyncio.sleep(2)

        # for each created table, need to send a /db/database/table/sync 
        # which includes copy of latest table & last_txn_time

        new_or_stale_endpoints = {}
        new_or_stale_endpoints.update(table_endpoints['new'])
        new_or_stale_endpoints.update(table_endpoints['stale'])

        table_copy = None

        sync_requests = {} 
        for _endpoint in new_or_stale_endpoints:
            if not _endpoint in alive_endpoints:
                continue
            endpoint = new_or_stale_endpoints[_endpoint]
            
            # avoid pulling table copy twice
            if table_copy == None:
                table_copy = await cluster_table_copy(
                    pyql, 
                    table, 
                    copy_only=True,
                    **kw
                )
                # trigger table creation on new_endpoint
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']
            token = endpoint['token']

            sync_requests[epuuid] = {
                'path': f"http://{path}/db/{db}/table/{table}/sync",
                'data': table_copy,
                'timeout': 2.0,
                'headers': await get_auth_http_headers('remote', token=token),
                'session': await get_endpoint_sessions(epuuid, **kw)
            }
            if table == 'state':
                # Allowing only 1 state table sync per job, to avoid state mismatches
                break
        sync_table_results = await async_request_multi(
            sync_requests, 
            'POST', 
            loop=loop
        )
        trace(f"sync_table_results: {sync_table_results}")

        # mark loaded
        mark_loaded = []
        for endpoint in sync_table_results:
            if not sync_table_results[endpoint]['status'] == 200:
                continue
            set_loaded = {
                'set': {
                    'state': 'loaded',
                    'info': {}
                    },
                'where': {
                    'name': f"{endpoint}_{table}"
                    }
            }
            mark_loaded.append(
                cluster_table_change(
                    pyql,
                    'state',
                    'update',
                    set_loaded,
                    force=True,
                    trace=trace,
                    loop=loop
                )
            )
            # update stale - 'state' endpoint 
            if table == 'state':
                stale_state_update = {}
                _endpoint = new_or_stale_endpoints[endpoint]
                db = _endpoint['db_name']
                path = _endpoint['path']
                epuuid = _endpoint['uuid']
                token = _endpoint['token']

                stale_state_update[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/state/update",
                    'data': set_loaded,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
                mark_loaded.append(
                    async_request_multi(
                        stale_state_update, 
                        'POST', 
                        loop=loop
                    )
                )

        mark_loaded_results = await asyncio.gather(
            *mark_loaded,
            return_exceptions=True
        )
        trace(f"mark_loaded_results: {mark_loaded_results}")
        

        # post sync - unpause
        unpause_table = await table_pause(pyql, table, 'stop', **kw)
        trace(f"unpause_table - {unpause_table}")
        return {
            "sync_table_results": sync_table_results,
            "mark_loaded": mark_loaded_results
        }

    @server.trace
    async def table_sync_run(cluster=None, table=None, config=None, job=None, **kw):
        """
        called by a job created by tablesync_mgr

        runs in following conditions:
        - new endpoint joins a cluster which creates 'new' tables matching existing 
        - existing endpoint re-joins cluster (from instance restart) if endpoint is 'stale'
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID']

        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        loop = kw['loop']

        sync_results = {}
        if cluster == None or table == None or job == None:
            cluster, table, job = (
                config['cluster'],
                config['table'],
                config['job']
            )

        trace(f"starting for cluster: {cluster} table {table}")

        # get list of table endpoints that are not 'loaded'
        table_endpoints = await get_table_endpoints(cluster, table, **kw)
        cluster_type = table_endpoints['cluster_type']

        new_or_stale_endpoints = table_endpoints['new']
        new_or_stale_endpoints.update(table_endpoints['stale'])

        all_table_endpoints = {}

        for state in ['loaded', 'stale', 'new']:
            all_table_endpoints.update(table_endpoints[state])

        # verify endpoints are alive
        check_alive_endpoints = await get_alive_endpoints(
            all_table_endpoints,
            **kw
        )
        alive_endpoints = []
        for endpoint in check_alive_endpoints:
            if not check_alive_endpoints[endpoint]['status'] == 200:
                continue
            alive_endpoints.append(endpoint)
        
        # Log Cluster Tables Workflow 
        if cluster_type == 'log':
            return await txn_table_sync(
                cluster, 
                table, 
                alive_endpoints, 
                table_endpoints,
                **kw
            )

        # create tables on new endpoints
        if len(table_endpoints['new']) > 0:
            table_config = None
            create_requests = {} 
            for _new_endpoint in table_endpoints['new']:
                if not _new_endpoint in alive_endpoints:
                    continue
                new_endpoint = table_endpoints['new'][_new_endpoint]
                
                # avoid pulling table config twice
                table_config = await cluster_table_config(
                    cluster, table, **kw
                    ) if table_config == None else table_config 
            
                # trigger table creation on new_endpoint
                db = new_endpoint['db_name']
                path = new_endpoint['path']
                epuuid = new_endpoint['uuid']
                token = new_endpoint['token']

                create_requests[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/{table}/create",
                    'data': table_config,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            create_table_results = await async_request_multi(
                create_requests, 
                'POST', 
                loop=loop
            )
            trace(f"{cluster} {table} {job} create_table_results: {create_table_results}")

            for endpoint in create_table_results:
                if not create_table_results[endpoint]['status'] == 200:
                    del table_endpoints['new'][endpoint]


            if cluster == pyql and table in ['jobs', 'tables', 'state']:
                return await pyql_state_sync_run(
                    table, 
                    alive_endpoints,
                    table_endpoints,
                    **kw
                )

            # for each created table, need to send a /db/database/table/sync 
            # which includes copy of latest table & last_txn_time
            table_copy = None

            sync_requests = {} 
            for _new_endpoint in table_endpoints['new']:
                if not _new_endpoint in alive_endpoints:
                    continue
                new_endpoint = table_endpoints['new'][_new_endpoint]
                
                # avoid pulling table config twice
                if table_copy == None:
                    table_copy = await cluster_table_copy(
                        cluster, 
                        table, 
                        **kw
                    )
                    # trigger table creation on new_endpoint
                db = new_endpoint['db_name']
                path = new_endpoint['path']
                epuuid = new_endpoint['uuid']
                token = new_endpoint['token']

                sync_requests[epuuid] = {
                    'path': f"http://{path}/db/{db}/table/{table}/sync",
                    'data': table_copy,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            sync_table_results = await async_request_multi(
                sync_requests, 
                'POST', 
                loop=loop
            )
        # trigger flush ( first sync for new ) on stale & new endpoints
        # for all endpoints trigger /flush


        # gather flush config
        txn_cluster = await server.clusters.data_to_txn_cluster.select(
            'txn_cluster_id',
            where={'data_cluster_id': cluster}
        )
        txn_cluster_id = txn_cluster[0]['txn_cluster_id']

        tx_table = '_'.join(f"txn_{cluster}_{table}".split('-'))

        # create limited use token
        limited_use_token = await server.create_auth_token(
            cluster, # id
            time.time() + 300,
            'cluster',
            extra_data={
                'cluster_allowed': txn_cluster_id,
                'table_allowed': tx_table
            }
        )

        flush_config = {
            "tx_cluster_path": (
                f"http://{os.environ['PYQL_CLUSTER_SVC']}/cluster/{txn_cluster_id}_log/table/{tx_table}/select"
            ),
            "token": limited_use_token
        }


        # create signal endpoint config

        flush_requests = {}
        for _endpoint in new_or_stale_endpoints:
            if not _endpoint in alive_endpoints:
                continue
            endpoint = new_or_stale_endpoints[_endpoint]
                # trigger table creation on new_endpoint
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']
            token = endpoint['token']

            flush_requests[epuuid] = {
                'path': f"http://{path}/db/{db}/table/{table}/flush",
                'data': flush_config,
                'timeout': 2.0,
                'headers': await get_auth_http_headers('remote', token=token),
                'session': await get_endpoint_sessions(epuuid, **kw)
            }
        flush_results = await async_request_multi(flush_requests, 'POST', loop=loop)
        trace(f"{cluster} {table} {job} flush_results: {flush_results}")

        # mark table endpoint loaded
        state_updates = []
        state_update_results = []
        for endpoint in flush_results:
            if not flush_results[endpoint]['status'] == 200:
                continue
            state_update_results.append(
                await cluster_table_change(
                    pyql,
                    'state',
                    'update',
                    {
                        'set': {
                            'state': 'loaded',
                            'info': {}
                            },
                        'where': {
                            'name': f"{endpoint}_{table}"
                            }
                    },
                    trace=trace,
                    loop=loop
                )
            )
        #state_update_results = await asyncio.gather(*state_updates, loop=loop)
        trace(f"{cluster} {table} {job} state_update_results: {state_update_results}")
        await asyncio.sleep(10)

        for _endpoint in new_or_stale_endpoints:
            if not _endpoint in alive_endpoints:
                continue
            endpoint = new_or_stale_endpoints[_endpoint]
                # trigger table creation on new_endpoint
            db = endpoint['db_name']
            path = endpoint['path']
            epuuid = endpoint['uuid']
            if cluster == pyql:
                trace(f"checking if pyql endpoint can be marked ready=True")
                ready = True
                for table_state in await server.clusters.state.select(
                    'state', 'name', 
                    where={"uuid": epuuid}
                    ):
                    if table_state['state'] in ['new', 'stale']:
                        trace(f"{table_state['name']} is still {table_state['state']}, cannot mark endpoint ready")
                        ready=False
                        break
                # no tables for endpoint are in_sync false - mark endpoint ready = True
                if ready:
                    await update_cluster_ready(path=path, ready=True, **kw)

        return {"state_update_results": state_update_results, "flush_results": flush_results}

    server.clusterjobs['table_sync_run'] = table_sync_run
                    
    @server.api_route('/cluster/{job_type}/add', methods=['POST'])
    async def cluster_jobs_add(job_type: str, config: dict, request: Request):
        return await cluster_jobs_add(job_type, config,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_jobs_add(job_type, config, **kw):
        return await jobs_add(config=config, **kw)
        
    @server.trace
    async def jobs_add(config=None, **kw):
        """
        meant to be used by node workers which will load jobs into cluster job queue
        to avoiding delays from locking during change operations
        For Example:
        # Load a job into node job queue
        server.jobs.append({'job': 'job-name', ...})
        Or
        cluster_jobs_add('syncjobs', jobconfig, status='WAITING')

        """
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        trace=kw['trace']
        trace(f"called with config: {config} - {kw}")

        job = config
        trace.warning(f"jobs_add for job {job} started")
        job_id = f'{uuid.uuid1()}'
        job_type = job['job_type']
        job_insert = {
            'id': job_id,
            'name': job['job'],
            'type': job_type if not job_type == 'cluster' else 'jobs', # cluster is converted to jobs
            'status': 'queued' if not 'status' in kw else kw['status'],
            'action': job['action'],
            'config': job['config']
        }
        if job_type == 'cron':
            job_insert['next_run_time'] = str(float(time.time()) + job['config']['interval'])
        else:
            job_check = await table_select(
                pyql, 'jobs', 
                data={'select': ['id'], 'where': {'name': job['job']}},
                method='POST',
                **kw
                )
            if not job_check:
                return {"message": trace(
                            f'job {job} not added, could not verify if job exists in table, try again later'
                        )
                    }
            job_check= job_check['data']

            if len(job_check) > 0:
                job_status = f"job {job_check[0]['id'] }with name {job['job']} already exists"
                return {
                    'message': trace.warning(job_status),
                    'job_id': job_check[0]['id']
                }

        response = await cluster_table_change(pyql, 'jobs', 'insert', job_insert, **kw)
        trace.warning(f"cluster {job_type} add for job {job} finished - {response}")
        return {
            "message": f"job {job} added to jobs queue - {response}",
            "job_id": job_id}
    server.clusterjobs['jobs_add'] = jobs_add
    

    @server.api_route('/cluster/jobs/{jobtype}/run', methods=['POST'])
    async def cluster_job_check_and_run_api(jobtype: str, request: Request):
        log.info(jobtype)
        return await cluster_job_check_and_run(jobtype,  request=await server.process_request(request))
    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_job_check_and_run(job_type, **kw):
        return await job_check_and_run(job_type, **kw)

    @server.trace
    async def job_check_and_run(job_type, **kw):
        trace = kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        # try to pull job 
        job = await jobqueue(job_type, node_id, **kw)
        if not job or 'message' in job:
            trace(f"{job_type} - {job}")
            return job
        trace(f"{job_type} - job pulled {job['name']}")
        job_config = job['config']
        try:
            await job_update(
                job_type, 
                job['id'], 
                'running', 
                job_info={"message": f"starting {job['name']}"}, 
                **kw
                )
        except Exception as e:
            trace.exception(f"exception while marking job {job['name']} running")

        trace(f"running job with config {job_config}")
        result, error = None, None
        try:
            result = await server.clusterjobs[job['action']](config=job_config, **kw)
        except Exception as e:
            error = trace.exception(f"exception while running job {job['name']}")

        if result:
            await job_update(
                job_type, job['id'], 'finished', 
                job_info={
                    "message": f"finished {job['name']}",
                    "result": result
                    }, **kw)
        else:
            error = trace(f"Error while running job - {error}")
            await job_update(job_type, job['id'], 'queued', job_info={"error": f"{error} - requeuing"}, **kw)
            return {"error": error}
        if 'nextJob' in job_config:
            await job_update(job_type, job_config['nextJob'], 'queued', job_info={"message": f"queued after {job['name']} completed"}, **kw)
        trace(f"finished {job['name']} with result: {result}")
        return {"result": result}
        
    server.job_check_and_run = job_check_and_run

    if await server.env['SETUP_ID'] == server.setup_id:
        await server.internal_job_add(join_cluster_job)

        if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
            #Job to trigger cluster_quorum()
            """
            init_quorum = {
                "job": "init_quorum",
                "job_type": "cluster",
                "method": "POST",
                "action": 'cluster_quorum_update',
                "path": "/pyql/quorum",
                "config": {}
            }
            """
            init_mark_ready_job = {
                "job": "init_mark_ready",
                "job_type": "cluster",
                "method": "POST",
                "path": "/cluster/pyql/ready",
                "config": {'ready': True}
            }
            # Create Cron Jobs inside init node
            cron_jobs = []
            if not os.environ.get('PYQL_TYPE') == 'K8S':
                #await server.internal_job_add(init_quorum)
                #server.internal_job_add(initMarkReadyJob)
                cron_jobs.append({
                    'job': 'cluster_quorum_check',
                    'job_type': 'cron',
                    "action": 'cluster_quorum_check',
                    "config": {"interval": 15}
                })
            for i in [30,90]:
                cron_jobs.append({
                    'job': f'tablesync_check_{i}',
                    'job_type': 'cron',
                    "action": 'tablesync_mgr',
                    "config": {"interval": i}
                })
                cron_jobs.append({
                    'job': f'cluster_job_cleanup_{i}',
                    'job_type': 'cron',
                    'action': 'jobmgr_cleanup',
                    'config': {'interval': i}
                })
            for job in cron_jobs:
                new_cron_job = {
                    "job": f"add_cron_job_{job['job']}",
                    "job_type": 'cluster',
                    "action": "jobs_add",
                    "config": job,
                }
                log.warning(f"adding job {job['job']} to internaljobs queue")
                await server.internal_job_add(new_cron_job)
        
        if len(endpoints) == 1 or os.environ['PYQL_CLUSTER_ACTION'] == 'init':
            ready_and_quorum = True
            health = 'healthy'
        else:
            await server.clusters.state.update(state='stale', where={'uuid': node_id})
            ready_and_quorum = False
            health = 'healing'
        # Sets ready false for any node with may be restarting as resync is required before marked ready

        await server.clusters.quorum.insert(**{
            'node': node_id,
            'nodes': {node_id: time.time()},
            'missing': {},
            'in_quorum': ready_and_quorum,
            'health': health,
            'last_update_time': float(time.time()),
            'ready': ready_and_quorum,
        })