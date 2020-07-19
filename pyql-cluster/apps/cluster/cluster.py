async def run(server):
    from fastapi import Request
    from pydantic import BaseModel
    import asyncio
    from aiohttp import ClientSession
    from apps.cluster.asyncrequest import async_request_multi, async_get_request, async_post_request
    from datetime import datetime
    import time, uuid, random
    from random import randrange
    import json, os
    from apps.cluster import request_async
    log = server.log

    #used to store links to functions called by jobs
    server.clusterjobs = {}

    # used for request session references
    server.sessions = {}

    class Tracer:
        def __init__(self, name, root=None):
            self.name = name
            self.root = root
            self.start = time.time()
        def get_callers(self, path=''):
            if self.root == None:
                return f"{self.name}{path}"
            else:
                path = f" --> {self.name}{path}"
                return self.root.get_callers(path)
        def get_root_caller_duration(self):
            if self.root == None:
                return time.time() - self.start
            else:
                return self.root.get_root_caller_duration()
        def log(self, message):
            root_duration = self.get_root_caller_duration()
            local_duration = time.time() - self.start
            return f"{self.get_callers()} - {root_duration:.3f} - {local_duration:.3f}s - {message}"

        def debug(self, message):
            log.debug(self.log(message))
            return message
        def error(self, message):
            log.error(self.log(message))
            return message
        def warning(self, message):
            log.warning(self.log(message))
            return message
        def info(self, message):
            log.info(self.log(message))
            return message
        def exception(self, message):
            log.exception(self.log(message))
            return message
        def __call__(self, message): 
            log.warning(self.log(message))
            return message

    def trace(func):
        def traced(*args, **kwargs):
            func_name = f'{func}'.split(' ')[1].split('.')[-1]
            if not 'trace' in kwargs:
                kwargs['trace'] = Tracer(func_name)
            else:
                kwargs['trace'] = Tracer(func_name, kwargs['trace'])
            return func(*args, **kwargs)
        return traced
    server.trace = trace

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

    uuid_check = await server.data['cluster'].tables['pyql'].select('uuid', where={'database': 'cluster'})
    if len(uuid_check) > 0:
        for _,v in uuid_check[0].items():
            dbuuid = str(v)
    else:
        dbuuid = str(uuid.uuid1())
        await server.data['cluster'].tables['pyql'].insert({
            'uuid': dbuuid,
            'database': 'cluster', 
            'last_mod_time': time.time()
        })
    node_id = dbuuid
    

    os.environ['PYQL_ENDPOINT'] = dbuuid
    await server.env.set_item('PYQL_ENDPOINT', dbuuid)

    os.environ['HOSTNAME'] = '-'.join(os.environ['PYQL_NODE'].split('.'))

    if not 'PYQL_CLUSTER_ACTION' in os.environ:
        os.environ['PYQL_CLUSTER_ACTION'] = 'join'

    # Table created only if 'init' is passed into os.environ['PYQL_CLUSTER_ACTION']
    tables = []
    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        table_list = ['clusters', 'endpoints', 'tables', 'state', 'transactions', 'jobs', 'auth']
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
            if 'auth_children' in request.__dict__:
                for child_id in request.auth_children:
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
            request = kwargs['request']
            if not 'auth' in request.__dict__:
                log.error("authentication is required or missing, this should have been handled by is_authenticated")
                server.http_exception(500, "authentication is required or missing")
            """
            ('id', str, 'UNIQUE NOT NULL'),
            ('name', str),
            ('owner', str), # UUID of auth user who created cluster 
            ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            ('created_by_endpoint', str),
            ('create_date', str)
            """
            args = list(args)
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
            if not 'quorum' in kwargs:
                quorum = await cluster_quorum()
                kwargs['quorum'] = quorum
            quorum = kwargs['quorum']
            if not 'quorum' in quorum or quorum['quorum']['in_quorum'] == False:
                server.http_exception(
                    500,
                    log.error(f"cluster pyql node {os.environ['HOSTNAME']} is not in quorum - {quorum}")
                )
            # Quorum passed - check that state is in_sync
            node_quorum_state = await server.clusters.quorum.select(
                'quorum.nodes', 'quorum.in_quorum', 'state.in_sync', 
                join={'state': {'quorum.node': 'state.uuid'}}, 
                where={'state.table_name': 'state', 'quorum.node': f'{node_id}'}
                )
            if len(node_quorum_state) == 0 or node_quorum_state[0]['quorum.in_quorum'] == False:
                server.http_exception(
                    500,
                    log.error(f"cluster pyql node {os.environ['HOSTNAME']} is not in quorum {quorum}")
                )
            node_quorum_state = node_quorum_state[0]
            if node_quorum_state['state.in_sync'] == True:
                return await func(*args, **kwargs)
            else:
                pyql = await server.env['PYQL_UUID']
                log.warning("state.in_sync is False for node but this node is in_quorum = True")
                pyql_nodes = await server.clusters.endpoints.select('uuid', 'path', where={'cluster': pyql})
                headers = dict(request.headers)
                # pop header fields which should not be passed
                for h in ['Content-Length']:
                    headers.pop(h.lower())
                if not 'unsafe' in headers:
                    headers['unsafe'] = node_id
                else:
                    headers['unsafe'] = ','.join(headers['unsafe'].split(',') + [node_id])
                for node in pyql_nodes:
                    if node['uuid'] in headers['unsafe']:
                        continue
                    if not node['uuid'] in node_quorum_state['quorum.nodes']['nodes']:
                        log.warning(f"node {node} was not yet 'unsafe' but is not in_quorum - {node_quorum_state} -, marking unsafe and will try other, if any")
                        headers['unsafe'] = ','.join(headers['unsafe'].split(',') + [node_id])
                        continue
                    headers['Host'] = node['path']
                    url = request.url
                    request_options = {
                        "method": request.method, 
                        "headers": headers, 
                        "data": request.json, 
                        "session": await get_endpoint_sessions(node['uuid'], **kw)}
                    r, rc =  await probe(url, **request_options)
                    if rc == 200: 
                        return r, rc
                    log.error(f"{r} - {rc} - found when probing {url} {node} - options: {request_options} - marking unsafe and will try other, if any") 
                    headers['unsafe'] = ','.join(headers['unsafe'].split(',') + [node_id])
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
        async def session():
            async with ClientSession(loop=server.event_loop) as client:
                trace(f"started session for endpoint {endpoint}")
                while True:
                    status = yield client
                    if status == 'finished':
                        trace(f"finished session for endpoint {endpoint}")
                        break
        if not endpoint in server.sessions:
            server.sessions[endpoint] = session()
            return await server.sessions[endpoint].asend(None)
        return await server.sessions[endpoint].asend(endpoint)

    # attaching to global server var    
    server.get_endpoint_sessions = get_endpoint_sessions

    # creating initial session for this nodeq
    await get_endpoint_sessions(node_id)

    async def cleanup_session(endpoint):
        try:
            if endpoint in server.sessions:
                log.warning(f"removing session for endpoint {endpoint}")
                await server.sessions[endpoint].asend('finished')
        except StopAsyncIteration:
            del server.sessions[endpoint]
        return
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

        if not 'session' in kw:
            temp_session, temp_id = True, str(uuid.uuid1())
            session = await get_endpoint_sessions(temp_id)
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
                result = await async_get_request(session, request, loop=server.event_loop)
            else:
                request['probe']['data'] = data
                result = await async_post_request(session, request, loop=server.event_loop)
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
    async def bootstrap_pyql_cluster(config, **kw):
        """
            runs if this node is targeted by /cluster/pyql/join and pyql cluster does not yet exist
        """
        trace = kw['trace']
        trace.info(f"bootstrap starting for {config['name']} config: {config}")
        request = kw['request']
        async def get_clusters_data():
            # ('id', str, 'UNIQUE NOT NULL'),
            # ('name', str),
            # ('owner', str), # UUID of auth user who created cluster 
            # ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            # ('created_by_endpoint', str),
            # ('create_date', str)
            
            return {
                'id': str(uuid.uuid1()),
                'name': 'pyql',
                'owner': kw['authentication'],
                'access': {'allow': [kw['authentication']]},
                'key': server.encode(
                    os.environ['PYQL_CLUSTER_INIT_ADMIN_PW'],
                    key=await server.env['PYQL_CLUSTER_TOKEN_KEY']
                    ),
                'created_by_endpoint': config['name'],
                'create_date': f'{datetime.now().date()}'
                }
        def get_endpoints_data(cluster_id):
            return {
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
        def get_tables_data(table,cluster_id, cfg, consistency):
            return {
                'id': str(uuid.uuid1()),
                'name': table,
                'cluster': cluster_id,
                'config': cfg,
                'consistency': consistency,
                'is_paused': False
            }
        def get_state_data(table, cluster_id):
            return {
                'name': f'{config["database"]["uuid"]}{table}',
                'state': 'loaded',
                'in_sync': True,
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
        return await probe(f"http://{path}/cluster/pyql/ready", method='POST', data={'ready': ready})

    server.clusterjobs['update_cluster_ready'] = update_cluster_ready
    @server.trace
    async def cluster_endpoint_delete(cluster=None, endpoint=None, config=None, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']
        if not config == None:
            cluster, endpoint = config['cluster'], config['endpoint']
        trace.error(f"cluster_endpoint_delete called for cluster - {cluster}, endpoint - {endpoint}")
        delete_where = {'where': {'uuid': endpoint, 'cluster': cluster}}
        results = {}
        results['state'] = await post_request_tables(pyql, 'state', 'delete', delete_where)
        results['endpoints'] = await post_request_tables(pyql, 'endpoints', 'delete', delete_where)
        results['transactions'] = await post_request_tables(pyql, 'transactions', 'delete', {'where': {'cluster': cluster, 'endpoint': endpoint}})
        return {"message": trace(f"deleted {endpoint} successfully - results {results}")}
    server.clusterjobs['cluster_endpoint_delete'] = cluster_endpoint_delete

    @server.trace
    async def get_alive_endpoints(endpoints, timeout=2.0, **kw):
        trace = kw['trace']
        ep_requests = {}
        for endpoint in endpoints:
            if endpoint['uuid'] == node_id:
                # no need to check own /pyql/node 
                continue 
            ep_requests[endpoint['uuid']] = {
                'path': f"http://{endpoint['path']}/pyql/node",
                'timeout':timeout,
                'session': await get_endpoint_sessions(endpoint['uuid'], **kw)
            }
        try:
            ep_results = await async_request_multi(ep_requests, loop=server.event_loop)
        except Exception as e:
            server.http_exception(
                500, trace.exception(f"Unhandled Excepton found during get_alive_endpoints"))
        for endpoint_id, response in ep_results.items():
            if response['status'] == 408:
                log.warning(f"observed timeout with endpoint {endpoint_id}, triggering cleanup of session")
                await cleanup_session(endpoint_id)
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
        pyql = await server.env['PYQL_UUID']
        trace.warning(f"received cluster_quorum_check for cluster {pyql}")
        pyql_endpoints = await server.clusters.endpoints.select('*', where={'cluster': pyql})
        if len(pyql_endpoints) == 0:
            return {
                "message": trace.warning(
                    "cluster_quorum_check found no pyql_endpoints, cluster may still be initializing")
                    }
        quorum = await server.clusters.quorum.select('*')
        # Check which pyql_endpoints are alive   
        alive_endpoints = await get_alive_endpoints(pyql_endpoints, trace=trace)
        alive_endpoints_nodes = [node_id]
        for endpoint in alive_endpoints:
            if alive_endpoints[endpoint]['status'] == 200:
                alive_endpoints_nodes.append(endpoint)
        # Compare live endpoints to current quorum 
        latestQuorumNodes = quorum[0]['nodes']['nodes']
        #if len(latestQuorumNodes) == len(alive_endpoints_nodes):
            #trace.warning("cluster_quorum_check completed, no detected quorum changes")
            # check each node to ensure quorum 
        if len(alive_endpoints_nodes) / len(pyql_endpoints) < 2/3: 
            quorum = {'alive': alive_endpoints_nodes, 'members': pyql_endpoints}
            server.http_exception(
                500, 
                trace.warning(f" detected node {node_id} is outOfQuorum - quorum {quorum}"))

        trace.warning(f"cluster_quorum_check detected quorum change, triggering update on alive_endpoints_nodes {alive_endpoints_nodes}")
        #quorumNodes = {q['node']: q for q in quorum}

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
            ep_results = await async_request_multi(ep_requests, 'POST', loop=server.event_loop)
        except Exception as e:
            trace.exception("Excepton found during cluster_quorum() check")
        trace.warning(f"cluster_quorum_check - results {ep_results}")

        return {
            "message": trace(f"cluster_quorum_check completed on {node_id}"), 
            'results': ep_results
            }
    server.clusterjobs['cluster_quorum_check'] = cluster_quorum_check
        
    @server.api_route('/pyql/quorum', methods=['GET', 'POST'])
    async def cluster_quorum_query(request: Request):
        return await cluster_quorum_query( request=await server.process_request(request))
    @server.is_authenticated('local')
    @server.trace
    async def cluster_quorum_query(check=False, get=False, **kw):
        trace=kw['trace']
        request = kw['request']
        if request.method == 'POST':
            #return cluster_quorum(check, get)
            return await cluster_quorum_update(trace=kw['trace'])
        return {'quorum': await server.clusters.quorum.select('*', where={'node': node_id})}

    @server.trace
    async def cluster_quorum_update(**kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']
        endpoints = await server.clusters.endpoints.select('*', where={'cluster': pyql})
        if len(endpoints) == 0:
            # may be a new node / still syncing
            return {"message": trace(f"cluster_quorum_update node {node_id} is still syncing")}
        if len(endpoints) == 1:
            health = 'healthy'
        pre_quorum = await server.clusters.quorum.select('*', where={'node': node_id})
        pre_quorum = pre_quorum[0]
        trace(f"pre_quorum check - {pre_quorum}")
        ep_results = await get_alive_endpoints(endpoints, trace=trace)
        # Check results
        quorum_to_update = {}
        in_quorum_nodes = []
        missing_nodes = []
        missing_nodes_times = []
        for endpoint in ep_results:
            if ep_results[endpoint]['status'] == 200:
                in_quorum_nodes.append(endpoint)
            else:
                missing_nodes.append(endpoint)
        # Quorum always assume local checking node is alive
        if not node_id in in_quorum_nodes:
            in_quorum_nodes.append(node_id)
        in_quorum = False
        if len(in_quorum_nodes) / len(endpoints) >= 2/3:
            in_quorum = True
            # updating missing nodes - if any - only if inQuroum
            if 'nodes' in pre_quorum['missing']:
                for node in pre_quorum['missing']['nodes']:
                    # check if pre missing node is still missing
                    if node['uuid'] in missing_nodes: 
                        if time.time() - node['time'] >= 45:
                            # create job to delete missing node
                            job = {
                                'job': f"delete_missing_node_{node['uuid']}",
                                'job_type': 'cluster',
                                'action': 'cluster_endpoint_delete',
                                'config': {
                                    'cluster': pyql, 
                                    'endpoint': node['uuid']
                                }
                            }
                            trace(f"adding job to delete missing node: {node['uuid']} - missing for more than 180 s")
                            await jobs_add(job, trace=trace)
                        missing_nodes.pop(missing_nodes.index(node['uuid']))
                        missing_nodes_times.append(node)
            for node in missing_nodes:
                missing_nodes_times.append({'uuid': node, 'time': float(time.time())})
        else:
            # mark this endpoint state OutOfsync

            # create an internal jobs to add to clusterJobQueue once quorum is re-establised
            # 1 - mark this endpoints state table OutOfSync globally
            health = 'unhealthy'
            quorum_to_update['ready'] = False
        # Quorum was previously not ready & outOfQuorum 
        if pre_quorum['health'] == 'unhealthy' and in_quorum == True:
            health = 'healing'
            job = {
                'job': f"{node_id}_mark_state_out_of_sync",
                'job_type': 'cluster',
                'action': 'table_update',
                'config': {
                    'cluster': pyql, 
                    'table': 'state', 
                    'data': {
                        'set': {'in_sync': False},
                        'where': {'uuid': node_id, 'table_name': 'state'}}
                    }
            }
            mark_state_out_of_sync_job = {
                "job": f"add_job_{node_id}_mark_state_out_of_sync",
                "job_type": 'cluster',
                "action": "jobs_add",
                "config": job,
            }
            trace.warning(f"This node was unhealthy, but started healing - adding job {mark_state_out_of_sync_job} to internaljobs queue")
            await server.internal_job_add(mark_state_out_of_sync_job)
        if pre_quorum['health'] in ['healing', 'healthy'] and in_quorum == True: 
            if pre_quorum['ready'] == True:
                health = 'healthy'
            else:
                health = 'healing'
                # check healing job
                healing_job = await server.clusters.jobs.select('*', where={'name': f'mark_ready_{node_id}'})
                if len(healing_job) == 1:
                    trace(f"quorum is currently healing using job: {healing_job}")
                    update_job_config = healing_job[0]['config']
                    for endpoint in endpoints:
                        if endpoint['uuid'] == node_id:
                            if not update_job_config['path'] == endpoint['path']:
                                trace("current healing job endpoint path is incorrect, updating job and requeing")
                                update_job_config['path'] = endpoint['path']
                                await post_request_tables(
                                    pyql, 
                                    'jobs', 
                                    'update', 
                                    {'set': {'config': update_job_config, 'status': 'queued'}, 'where': {'name': f'mark_ready_{node_id}'}},
                                    trace=trace
                                    )
        quorum_to_update.update({
            'in_quorum': in_quorum, 
            'health': health, 
            'nodes': {"nodes": in_quorum_nodes},
            'missing': {'nodes': missing_nodes_times},
            'last_update_time': float(time.time())
            })
        await server.clusters.quorum.update(
            **quorum_to_update,
            where={'node': node_id}
        )
        quorum_select = await server.clusters.quorum.select('*', where={'node': node_id})
        return {
            "message": trace(f"cluster_quorum_update on node {node_id} updated successfully"),
            'quorum': quorum_select[0]}
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
        paths = {'in_sync': {}, 'out_of_sync': {}}
        table_endpoints = await get_table_endpoints(cluster, table, caller='get_db_table_path', trace=kw['trace'])
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
    async def get_table_endpoints(cluster, table, cluster_name=None, caller=None, **kw):
        """
        Usage:
            get_table_endpoints('cluster_uuid', 'table_name')
        """
        trace = kw['trace']

        # pull cluster_name if None
        if cluster_name == None:
            cluster_name = await server.clusters.clusters.select(
                'name', where={'id': cluster}
            )
            cluster_name = cluster_name[0]['name']

        table_endpoints = {'in_sync': {}, 'out_of_sync': {}}

        endpoints = await server.clusters.endpoints.select(
            '*', 
            join={'state': {'endpoints.uuid': 'state.uuid', 'endpoints.cluster': 'state.cluster'}}, 
            where={'state.cluster': cluster, 'state.table_name': table}
            )
        
        endpointsKeySplit = []
        for endpoint in endpoints:
            renamed = {}
            for k,v in endpoint.items():
                renamed[k.split('.')[1]] = v
            endpointsKeySplit.append(renamed)
        for endpoint in endpointsKeySplit:
            sync = 'in_sync' if endpoint['in_sync'] == True else 'out_of_sync'
            table_endpoints[sync][endpoint['uuid']] = endpoint
        if not cluster_name == None:
            table_endpoints['cluster_name'] = cluster_name
        trace.warning(f"{caller} --> get_table_endpoints result {table_endpoints}")
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

        tables = await server.clusters.tables.select(
            '*',
            where={'cluster': cluster, 'name': table})
        for tb in table:
            trace(f"get_table_info_get_tables {tb}")
            tb['endpoints'] = {}
            for sync in ['in_sync', 'out_of_sync']:
                for endpoint in endpoints[sync]:
                    path = endpoints[sync][endpoint]['path']
                    db = endpoints[sync][endpoint]['db_name']
                    name = endpoints[sync][endpoint]['name']
                    tb['endpoints'][name] = endpoints[sync][endpoint]
                    tb['endpoints'][name]['path'] = f"http://{path}/db/{db}/table/{tb['name']}"
            trace(f"completed {tb}")
            return tb
        server.http_exception(500, f"no tables in cluster {cluster} with name {table} - found: {tables}")
    @server.trace
    async def post_request_tables(cluster, table, action, request_data, **kw):
        """
            use details=True as arg to return tb['endpoints'] in response
        """
        trace = kw['trace']
        pyql_txn_exceptions = {'transactions', 'jobs', 'state', 'tables'}
        table_endpoints = await get_table_endpoints(cluster, table, caller='post_request_tables', trace=kw['trace'])
        pyql = await server.env['PYQL_UUID']
        fail_track = []
        tb = await get_table_info(cluster, table, table_endpoints, trace=kw['trace'])
        async def process_request():
            endpoint_response = {}
            ep_requests = {}
            change_logs = {'txns': []}
            request_uuid = str(uuid.uuid1())
            transTime = time.time()
            def get_txn(endpoint_uuid):
                return {
                    'endpoint': endpoint_uuid,
                    'uuid': request_uuid,
                    'table_name': table,
                    'cluster': cluster,
                    'timestamp': transTime,
                    'txn': {action: request_data}
                }
            for endpoint in table_endpoints['in_sync']:
                db = table_endpoints['in_sync'][endpoint]['db_name']
                path = table_endpoints['in_sync'][endpoint]['path']
                epuuid = table_endpoints['in_sync'][endpoint]['uuid']
                #db = get_db_name(cluster, endpoint)
                token = table_endpoints['in_sync'][endpoint]['token']
                if tb['consistency'] == False:
                    # not caching - sending to DB ASAP
                    request_path = get_endpoint_url(path, action, trace=trace)
                    data = request_data
                else:
                    request_path = get_endpoint_url(path, action, cache=request_uuid, trace=trace)
                    data = {'txn': request_data, 'time': transTime}
                ep_requests[endpoint] = {
                    'path': request_path,
                    'data': data,
                    'timeout': 2.0,
                    'headers': await get_auth_http_headers('remote', token=token),
                    'session': await get_endpoint_sessions(epuuid, **kw)
                }
            async_results = await async_request_multi(ep_requests, 'POST', loop=server.event_loop)
            # async_results response format
            # [{'9f5f3600-c492-11ea-9ada-f3f7bc2ffe6c': {'content': {'message': 'items added'}, 'status': 200}}]
            trace(f"async_results - {async_results}")
            for endpoint in table_endpoints['in_sync']:
                if not async_results[endpoint]['status'] == 200:
                    fail_track.append(f'{endpoint}{table}')
                    # start job to Retry for endpoint, or mark endpoint bad after testing
                    error, rc = async_results[endpoint]['content'], async_results[endpoint]['status']
                    trace.warning(f"unable to {action} from endpoint {endpoint} using {request_data} - error: {error} rc: {rc}")
                else:
                    endpoint_response[endpoint] = async_results[endpoint]['content']
                    response, rc = async_results[endpoint]['status'], async_results[endpoint]['status']
            
            # At least 1 success in endpoint db change, need to mark failed endpoints out of sync
            # and create a changelog for resync
            if len(fail_track) > 0 and len(fail_track) < len(table_endpoints['in_sync']):
                trace.warning(f"At least 1 successful response & at least 1 failure to update of in_sync endpoints {fail_track}")
                for failed_endpoint in fail_track:
                    # Marking failed_endpoint in_sync=False for table endpoint
                    state_set = {
                        "set": {"in_sync": False},
                        "where": {"name": failed_endpoint}
                    }
                    if cluster == pyql and table == 'state' and 'state' in failed_endpoint:
                        # this is a pyql <endpoint>state table that is out_of_sync,
                        ep_state_requests = {}
                        for endpoint in endpoint_response:
                            db = table_endpoints['in_sync'][endpoint]['db_name']
                            path = table_endpoints['in_sync'][endpoint]['path']
                            token = table_endpoints['in_sync'][endpoint]['token']
                            epuuid = table_endpoints['in_sync'][endpoint]['uuid']
                            ep_state_requests[endpoint] = {
                                'path': get_endpoint_url(path, action, trace=trace),
                                'data': state_set,
                                'timeout': 2.0,
                                'headers': await get_auth_http_headers('remote', token=token),
                                'session': await get_endpoint_sessions(epuuid, **kw)
                            }
                        trace(f"marking {failed_endpoint} as in_sync=False on alive pyql state endpoints")
                        ep_state_results = await async_request_multi(ep_state_requests, 'POST', loop=server.event_loop)
                        trace(f"marking {failed_endpoint} as in_sync=False on alive pyql state endpoints - results: {ep_state_results}")
                    else:
                        state_set = {
                            "set": {"in_sync": False},
                            "where": {"name": failed_endpoint}
                        }
                        await post_request_tables(pyql, 'state', 'update', state_set, trace=trace)
                    # Creating txn log for future replay for table endpoint
                    if not tb['endpoints'][failed_endpoint]['state'] == 'new':
                        if cluster == pyql and table in pyql_txn_exceptions:
                            trace.warning(f"{failed_endpoint} is out_of_sync for pyql table {table}")
                            continue
                        # Write data to a change log for resyncing
                        change_logs['txns'].append(
                            get_txn(tb['endpoints'][failed_endpoint]['uuid'])
                        )
            # All endpoints failed request - 
            elif len(fail_track) == len(table_endpoints['in_sync']):
                error=f"All endpoints failed request {fail_track} using {request_data} thus will not update logs" 
                return {"message": trace.error(error), "results": async_results}
            else:
                # No InSync failures
                pass
            # Update any previous out of sync table change-logs, if any
            for out_of_sync_endpoint in table_endpoints['out_of_sync']:
                tb_endpoint = f'{out_of_sync_endpoint}{table}'
                if not tb_endpoint in tb['endpoints']:
                    trace(f"out_of_sync_endpoint {tb_endpoint} may be new, not triggering resync yet {tb['endpoints']}")
                    continue
                if not tb['endpoints'][tb_endpoint]['state'] == 'new':
                    # Prevent writing transaction logs for failed transaction log changes
                    if cluster == pyql and table in pyql_txn_exceptions:
                        continue 
                    trace(f"new out_of_sync_endpoint {tb_endpoint} need to write to db logs")
                    change_logs['txns'].append(
                        get_txn(tb['endpoints'][tb_endpoint]['uuid'])
                    )
                else:
                    trace(f"post_request_tables  table is new {tb['endpoints'][tb_endpoint]['state']}")
            
            async def write_change_logs(change_logs):
                if len(change_logs['txns']) > 0:
                    # Better solution - maintain transactions table for transactions, table sync and logic is already available
                    for txn in change_logs['txns']:
                        await post_request_tables(pyql,'transactions','insert', txn, trace=trace)
            if cluster == pyql and table in pyql_txn_exceptions:
                pass
            else:
                await write_change_logs(change_logs)
            # Commit cached commands  
            ep_commit_requests = {}
            if tb['consistency'] == True:
                for endpoint in endpoint_response:
                    #db = get_db_name(cluster, endpoint)
                    db = table_endpoints['in_sync'][endpoint]['db_name']
                    path = table_endpoints['in_sync'][endpoint]['path']
                    token = table_endpoints['in_sync'][endpoint]['token']
                    epuuid = table_endpoints['in_sync'][endpoint]['uuid']
                    ep_commit_requests[endpoint] = {
                        'path': get_endpoint_url(path, action, commit=True, trace=trace),
                        'data': endpoint_response[endpoint],
                        'timeout': 2.0,
                        'headers': await get_auth_http_headers('remote', token=token, trace=trace),
                        'session': await get_endpoint_sessions(epuuid, **kw)
                    }
                
                async_results = await async_request_multi(ep_commit_requests, 'POST', loop=server.event_loop)
                trace.info(async_results)
                # if a commit fails - due to a timeout or other internal - need to mark endpoint OutOfSync
                success, fail = set(), set()
                for endpoint in async_results:
                    if not async_results[endpoint]["status"] == 200:
                        fail.add(endpoint)
                    else:
                        success.add(endpoint)
                if len(success) == 0:
                    return {
                        "message": trace.error(f"failed to commit {request_data} to all in_sync {table} endpoints"), 
                        "details": async_results}, 400
                if len(fail) > 0:
                    trace.warning(f"commit failure for endpoints {fail}")
                    for endpoint in fail:
                        state_set = {
                            "set": {"in_sync": False, "state": 'new'},
                            "where": {"name": f"{endpoint}{table}"}
                        }
                        alert = f"failed to commit {request_uuid} in endpoint {endpoint}{table}, marking out_of_sync & state 'new'"
                        trace(f"{alert} as chain is broken")
                        fr, frc = await post_request_tables(pyql, 'state', 'update', state_set, trace=trace)
                        trace(f"{alert} - result: {fr} {frc}")

            return {"message": async_results, "consistency": tb['consistency']}
        if tb['is_paused'] == False:
            return await process_request()
        else:
            if cluster == pyql and table == 'tables' or table == 'state' and action == 'update':
                # tables val is_paused / state in_sync are values and we need to allow tables updates through if updating
                return await process_request()
            total_sleep = 0.5
            sleep = 0.5
            for _ in range(9): # waits up to 9 X sleep value - if paused
                trace.error(f"Table {table} is paused, Waiting {sleep} seconds before retrying - total wait time {total_sleep}")
                time.sleep(sleep)
                table_endpoints = await get_table_endpoints(cluster, table, caller='post_request_tables', trace=kw['trace'])
                tb = await get_table_info(cluster, table, table_endpoints, trace=kw['trace'])
                #TODO - create a counter stat to track how often this occurs
                if tb['is_paused'] == False:
                    return await process_request()
                total_sleep+=0.5
            error = "table is paused preventing changes, maybe an issue occured during sync cutover, try again later"
            server.http_exception(500, trace.error(error))

    @server.trace
    async def pyql_reset_jobs_table(**kw):
        trace = kw['trace']
        """
        this func should run if the cluster is in the following conditions:
        - Is IN Quorum - at least 2/3 nodes are active
        - The only in_sync i.e 'source of truth' for table is offline / outOfQuorum / path has changed
        """
        trace.warning(f"pyql_reset_jobs_table starting")
        pyql = await server.env['PYQL_UUID']
        update_where = {'set': {'in_sync': False}, 'where': {'table_name': 'jobs', 'cluster': pyql}}
        await post_request_tables(pyql, 'state', 'update', update_where, trace=trace)
        # delete all non-cron jobs in local jobs tb
        for jType in ['jobs', 'syncjobs']:
            delete_where = {'where': {'type': jType}}
            await server.clusters.jobs.delete(**delete_where)
         # set this nodes' jobs table in_sync=true
        update_where = {'set': {'in_sync': True}, 'where': {'uuid': node_id, 'table_name': 'jobs'}}
        await post_request_tables(pyql, 'state', 'update', update_where, trace=trace)
        trace.warning(f"pyql_reset_jobs_table finished")

    @server.trace
    async def table_select(cluster, table, **kw):
        return await endpoint_probe(cluster, table, '/select', **kw)
    server.cluster_table_select = table_select

    @server.trace
    async def get_random_table_endpoint(cluster, table, quorum=None, **kw):
        trace = kw['trace']
        pyql = await server.env['PYQL_UUID']
        endpoints = await get_table_endpoints(cluster, table, caller='get_random_table_endpoint', trace=trace)
        endpoints = endpoints['in_sync']
        in_sync_endpoints = [ep for ep in endpoints]
        if len(in_sync_endpoints) == 0 and table == 'jobs':
            await pyql_reset_jobs_table()
            endpoints = await get_table_endpoints(cluster, table, caller='get_random_table_endpoint', trace=trace)
            endpoints = endpoints['in_sync']
            in_sync_endpoints = [ep for ep in endpoints]
        while len(in_sync_endpoints) > 0: 
            if node_id in in_sync_endpoints:
                endpoint_choice = in_sync_endpoints.pop(in_sync_endpoints.index(node_id))
            else:
                if len(in_sync_endpoints) > 1:
                    endpoint_choice = in_sync_endpoints.pop(randrange(len(in_sync_endpoints)))
                else:
                    endpoint_choice = in_sync_endpoints.pop(0)
            if not quorum == None and cluster == pyql:
                if not endpoint_choice in quorum['quorum']['nodes']['nodes']:
                    trace.warning(f"get_random_table_endpoint skipped pyql endpoint {endpoint_choice} as not in quorum")
                    if len(in_sync_endpoints) == 0 and table == 'jobs':
                        await pyql_reset_jobs_table(trace=trace)
                        endpoints = await get_table_endpoints(cluster, table, caller='get_random_table_endpoint', trace=trace)['in_sync']
                        in_sync_endpoints = [ep for ep in endpoints]
                    continue
            yield endpoints[endpoint_choice]
        yield None
    #table_select(pyql, 'jobs', data=job_select, method='POST', **kw)    
    @server.trace
    async def endpoint_probe(cluster, table, path='', data=None, timeout=1.0, quorum=None, **kw):
        trace = kw['trace']
        request = kw['request'] if 'request' in kw else None
        errors = []
        method = request.method if not 'method' in kw else kw['method']
        if method in ['POST', 'PUT'] and data == None:
            server.http_exception(400, trace.error("expected json input for request"))
        async for endpoint in get_random_table_endpoint(cluster, table, quorum):
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
                    method=method,
                    data=data,
                    token=endpoint['token'],
                    timeout=timeout,
                    session=await get_endpoint_sessions(endpoint['uuid'], **kw)
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
        return await cluster_table_config(cluster, table,  request=await server.process_request(request))
    @state_and_quorum_check
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    async def cluster_table_config(cluster, table, **kw):
        return await table_config(cluster, table, **kw)
    
    @server.trace
    async def table_config(cluster, table, **kw):
        return await endpoint_probe(cluster, table, method='GET', path=f'/config', **kw)

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
            return table_update(cluster=cluster, table=table, data={'set': data, 'where': {primary: key}}, **kw)
        if request.method == 'DELETE':
            return table_delete(cluster, table, {'where': {primary: key}}, **kw)
    
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
        if not config == None: # this was invoked by a job
            cluster, table, data = config['cluster'], config['table'], config['data']
        return await post_request_tables(
            cluster, table,'update', data, trace=trace)
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
        return await post_request_tables(cluster, table, 'insert',  data, **kw)
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
        return await post_request_tables(cluster, table, 'delete', data, **kw)

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
        pyql = await server.env['PYQL_UUID']
        pause = True if pause == 'start' else False
        pause_set = {
            'set': {'is_paused': pause},
            'where': {'cluster': cluster, 'name': table}
        }
        result = await post_request_tables(pyql, 'tables', 'update', pause_set, trace=kw['trace'])
        if 'delay_after_pause' in kw:
            time.sleep(kw['delay_after_pause'])
        trace.warning(f'cluster_table_pause {cluster} {table} pause {pause} result: {result}')
        return result
        
    @server.trace
    async def table_endpoint(cluster, table, endpoint, config=None, **kw):
        trace=kw['trace']
        """ Sets state 
        cluster: uuid, table: name, endpoint: uuid, 
        config: {'in_sync': True|False, 'state': 'loaded|new'}
        last_mod_time: float(time.time())
        
        """
        pyql = await server.env['PYQL_UUID']
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
        result = await post_request_tables(pyql, 'state', 'update', sync_set, trace=kw['trace'])
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
            return await table_endpoint_logs(cluster, table, endpoint, action, trace=kw['trace'])
        if request.method == 'POST' and action == 'commit':
            return await commit_table_endpoint_logs(cluster, table, endpoint, txns=data, trace=kw['trace'])

    @server.trace
    async def table_endpoint_logs(cluster, table, endpoint, action, **kw):
        """
        requires cluster uuid for cluster
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID']
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
            server.http_exception(500, log.error("error pulling endpoint logs"))
        if action == 'count':
            log.warning(f"# count completed")
            return {"availableTxns": len(response['data'])}
        elif action == 'getAll':
            log.warning(f"# getAll completed")
            return response
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
        pyql = await server.env['PYQL_UUID']
        for txn in txns['txns']:
            delete_txn = {
                'where': {
                    'endpoint': endpoint,
                    'cluster': cluster,
                    'table_name': table,
                    'uuid': txn
                }
            }
            resp = await post_request_tables(pyql, 'transactions', 'delete', delete_txn, trace=kw['trace'])
        return {"message": trace(f"successfully commited txns {txns}")}


    @server.api_route('/cluster/{cluster_name}/join', methods=['GET','POST'])
    async def join_cluster_api(cluster_name: str, config: dict, request: Request):
        return await join_cluster(cluster_name, config,  request=await server.process_request(request))

    @server.is_authenticated('cluster')
    @server.trace
    async def join_cluster(cluster_name, config, **kw):
        trace=kw['trace']
        request = kw['request']
        required = {
            "name": "endpoint-name",
            "path": "path-to-endpoint",
            "database": {
                'name': "database-name",
                'uuid': "uuid"
            },
            "tables": [
                {"tb1": 'json.dumps(conf)'},
                {"tb2": 'json.dumps(conf)'},
                {"tb3": 'json.dumps(conf)'}
            ],
            "consistency": ['tb1', 'tb2'] # defines if table modifications are cached & txn time is reviewed before submission
        }
        if request.method=='GET':
            return required
        else:
            trace.info(f"join cluster for {cluster_name} with kwargs {kw}")
            if not 'consistency' in config:
                config['consistency'] = []
            db = server.data['cluster']
            new_endpoint_or_database = False
            jobs_to_run = []
            is_pyql_bootstrapped = False
            pyql = None

            clusters = await server.clusters.clusters.select(
                    '*', where={'name': 'pyql'})
            for cluster in clusters:
                if cluster['name'] == 'pyql':
                     is_pyql_bootstrapped, pyql = True, cluster['id']

            if not is_pyql_bootstrapped and cluster_name == 'pyql':
                await bootstrap_pyql_cluster(config, **kw)
            
            clusters = await server.clusters.clusters.select(
                '*', where={'name': 'pyql'}
            )
            

            clusters = await server.clusters.clusters.select(
                '*', where={'owner': kw['authentication']})
            if not cluster_name in [cluster['name'] for cluster in clusters]:
                #Cluster does not exist, need to create
                # ('id', str, 'UNIQUE NOT NULL'),
                # ('name', str),
                # ('owner', str), # UUID of auth user who created cluster 
                # ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
                # ('created_by_endpoint', str),
                # ('create_date', str)

                data = {
                    'id': str(uuid.uuid1()),
                    'name': cluster_name,
                    'owner': kw['authentication'], # added via @server.is_autenticated
                    'access': {'allow': [kw['authentication']]},
                    'created_by_endpoint': config['name'],
                    'create_date': f'{datetime.now().date()}'
                    }
                await post_request_tables(pyql, 'clusters', 'insert', data, trace=kw['trace'])
            cluster_id = await server.clusters.clusters.select(
                '*', where={
                        'owner': kw['authentication'], 
                        'name': cluster_name
                    })
            cluster_id = cluster_id[0]['id']
            
            #check for existing endpoint in cluster: cluster_id 
            endpoints = await server.clusters.endpoints.select('uuid', where={'cluster': cluster_id})
            if not config['database']['uuid'] in [endpoint['uuid'] for endpoint in endpoints]:
                #add endpoint
                new_endpoint_or_database = True
                data = {
                    'uuid': config['database']['uuid'],
                    'db_name': config['database']['name'],
                    'path': config['path'],
                    'token': config['token'],
                    'cluster': cluster_id
                }
                await post_request_tables(pyql, 'endpoints', 'insert', data, trace=kw['trace'])

            else:
                #update endpoint latest path info - if different
                trace.warning(f"endpoint with id {config['database']['uuid']} already exists in {cluster_name} {endpoints}")
                update_set = {
                    'set': {'path': config['path']},
                    'where': {'uuid': config['database']['uuid']}
                }
                if len(endpoints) == 1 and cluster_name == 'pyql':
                    #Single node pyql cluster - path changed
                    await server.clusters.endpoints.update(
                        **update_set['set'],
                        where=update_set['where']
                    )
                else:
                    await post_request_tables(pyql, 'endpoints', 'update', update_set, trace=kw['trace'])
                    if cluster_id == await server.env['PYQL_UUID']:
                        await post_request_tables(
                            pyql, 'state', 'update', 
                            {'set': {'in_sync': False}, 
                            'where': {
                                'uuid': config['database']['uuid'],
                                'cluster': cluster_id}}, trace=kw['trace'])
            tables = await server.clusters.tables.select('name', where={'cluster': cluster_id})
            tables = [table['name'] for table in tables]
            # if tables not exist, add
            newTables = []
            for table in config['tables']:
                for table_name, tb_config in table.items():
                    if not table_name in tables:
                        newTables.append(table_name)
                        #JobIfy - create as job so config
                        data = {
                            'id': str(uuid.uuid1()),
                            'name': table_name,
                            'cluster': cluster_id,
                            'config': tb_config,
                            'consistency': table_name in config['consistency'],
                            'is_paused': False
                        }
                        await post_request_tables(pyql, 'tables', 'insert', data, trace=kw['trace'])
            # If new endpoint was added - update endpoints in each table 
            # so tables can be created in each endpoint for new / exsting tables
            if new_endpoint_or_database == True:
                jobs_to_run = [] # Resetting as all cluster tables need a job to sync on new_endpoint_or_database
                tables = await server.clusters.tables.select('name', where={'cluster': cluster_id})
                tables = [table['name'] for table in tables]
                endpoints = await server.clusters.endpoints.select('*', where={'cluster': cluster_id})    
                state = await server.clusters.state.select('name', where={'cluster': cluster_id})
                state = [tbEp['name'] for tbEp in state]

                for table in tables:
                    for endpoint in endpoints:
                        tableEndpoint = f"{endpoint['uuid']}{table}"
                        if not tableEndpoint in state:
                            # check if this table was added along with endpoint, and does not need to be created 
                            load_state = 'loaded' if endpoint['uuid'] == config['database']['uuid'] else 'new'
                            if not table in newTables:
                                #Table arleady existed in cluster, but not in endpoint with same table was added
                                sync_state = False
                                load_state = 'new'
                            else:
                                # New tables in a cluster are automatically marked in sync by endpoint which added
                                sync_state = True
                            # Get DB Name to update
                            data = {
                                'name': tableEndpoint,
                                'state': load_state,
                                'in_sync': sync_state,
                                'table_name': table,
                                'cluster': cluster_id,
                                'uuid': endpoint['uuid'], # used for syncing logs
                                'last_mod_time': 0.0
                            }
                            await post_request_tables(pyql, 'state', 'insert', data, trace=kw['trace'])
            else:
                # Not bootrapping cluster, not a new endpoint or databse this is pyql cluster
                if is_pyql_bootstrapped and not new_endpoint_or_database and cluster_name == 'pyql':
                    if cluster_name == 'pyql':
                        await tablesync_mgr(**kw)
            # Trigger quorum update using any new endpoints if cluster name == pyql
            if cluster_name == 'pyql':
                await cluster_quorum_check()
                if not is_pyql_bootstrapped:
                    await set_pyql_id({'PYQL_UUID': cluster_id})
                else:
                # pyql setup - sets pyql_uuid in env 
                    await probe(
                        f"http://{config['path']}/pyql/setup",
                        method='POST',
                        data={'PYQL_UUID': cluster_id},
                        token=config['token'],
                        session=await get_endpoint_sessions(config['database']['uuid'], **kw)
                    )
                    # auth setup - applys cluster service token in joining pyql node, and pulls key
                    result, rc = await probe(
                        f"http://{config['path']}/auth/setup/cluster",
                        method='POST',
                        data={
                            'PYQL_CLUSTER_SERVICE_TOKEN': await server.env['PYQL_CLUSTER_SERVICE_TOKEN']
                        },
                        token=config['token'],
                        session=await get_endpoint_sessions(config['database']['uuid'], **kw)
                    )
                    trace.warning(f"completed auth setup for new pyql endpoint: result {result} {rc}")
            return {"message": trace.warning(f"join cluster {cluster_name} for endpoint {config['name']} completed successfully")}, 200
    @server.trace
    async def re_queue_job(job, **kw):
        await job_update(job['type'], job['id'],'queued', {"message": "job was requeued"}, trace=kw['trace'])

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
        pyql = await server.env['PYQL_UUID']
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
                    await post_request_tables(
                        pyql, 'jobs', 'update', 
                        {'set': {
                            'start_time': time.time()}, 
                        'where': {
                            'id': job['id']}}, trace=kw['trace'])
            if job['status'] == 'waiting':
                waiting_on = None
                for jb in jobs:
                    if 'nextJob' in jb['config']:
                        if jb['config']['nextJob'] == job['id']:
                            waiting_on = jb['id']
                            break
                if waiting_on == None:
                    trace.warning(f"Job {job['name']} was waiting on another job which did not correctly queue, queuing now.")
                    await re_queue_job(job, trace=kw['trace'])
                    
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
    async def jobqueue(job_type, node=None, **kw):
        """
            Used by jobworkers or tablesyncers to pull jobs from clusters job queues
            job_type = 'job|syncjob|cron'
        """
        trace = kw['trace']
        request = kw['request']
        queue = f'{job_type}s' if not job_type == 'cron' else job_type

        pyql = await server.env['PYQL_UUID']
        if pyql == None:
            return {"message": "cluster is still bootstrapping, try again later"}

        node = node_id
        for _ in range(2):
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
            job_list = await table_select(pyql, 'jobs', data=job_select, method='POST', **kw)
            if not job_list:
                return {"message": trace("unable to pull jobs at this time")}
            trace(f"finished pulling list of jobs - job_list {job_list} ")
            job_list = job_list['data']
            for i, job in enumerate(job_list):
                if not job['next_run_time'] == None:
                    #Removes queued job from list if next_run_time is still in future 
                    if not float(job['next_run_time']) < float(time.time()):
                        job_list.pop(i)
                    if not job['node'] == None:
                        if time.time() - float(job['next_run_time']) > 120.0:
                            trace(f"found stuck job assigned to node {job['node']} - begin re_queue job")
                            trace.error(f"job # {job['id']} may be stuck / inconsistent, updating to requeue")
                            await re_queue_job(job, trace=kw['trace'])
                            trace(f"found stuck job assigned to node {job['node']} - finished re_queue job")
                            #job_update = {'set': {'node': None}, 'where': {'id': job['id']}}
                            #await post_request_tables(pyql, 'jobs', 'update', job_update)

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

            job_update = {'set': {'node': node}, 'where': {'id': job['id'], 'node': None}}
            result = await post_request_tables(pyql, 'jobs', 'update', job_update, trace=kw['trace'])
            if not result:
                trace.error(f"failed to reserve job {job} for node {node}")
                return {"message": trace("no jobs to process at this time")}

            # verify if job was reserved by node and pull config
            job_select['where']['node'] = node
            job_select['select'] = ['*']
            job_check = await table_select(pyql, 'jobs', data=job_select, method='POST', **kw)
            if len(job_check['data']) == 0:
                continue
            trace.warning(f"cluster_jobqueue - pulled job {job_check['data'][0]} for node {node}")
            return job_check['data'][0]
        trace.warning(f"failed to reserve job {job} after 2 attempts for node {node}")
        return {"message": trace("no jobs to process at this time")}
        

    @server.api_route('/cluster/job/{job_type}/{job_id}/{status}', methods=['POST'])
    async def cluster_job_update_api(job_type: str, job_id: str, status: str, request: Request, job_info: dict = None):
        return await cluster_job_update(job_type, job_id, status, job_info=job_info,  request=await server.process_request(request))

    @state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_job_update(job_type, job_id, status, **kw):
        return await job_update(job_type, job_id, status, **kw)

    async def job_update(job_type, job_id, status, job_info={}, **kw):
        pyql = await server.env['PYQL_UUID']
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
                return await post_request_tables(pyql, 'jobs', 'update', update_from, trace=kw['trace']) 
            return await post_request_tables(pyql, 'jobs', 'delete', update_from)
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
            else:
                update_where['set']['start_time'] = str(time.time())
            return await post_request_tables(pyql, 'jobs', 'update', update_where, trace=kw['trace'])

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
        request = kw['request']
        #need to check quorum as all endpoints are currently in_sync = False for table
        pyql = await server.env['PYQL_UUID']
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
                trace=kw['trace']
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
                    trace=kw['trace']
                )
        else:
            await post_request_tables(pyql, 'state', 'update', update_set_in_sync, trace=kw['trace'])
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
            invoked regularly by cron or ondemand to create jobs to sync OutOfSync Endpoints.
        """
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID']
        jobs_to_create = {}
        jobs = {}
        tables = await server.clusters.tables.select('name', 'cluster')
        for table in tables:
            cluster = table['cluster']
            table_name = table['name']
            endpoints = await get_table_endpoints(cluster,table_name, caller='tablesync_mgr',trace=kw['trace'])
            if not len(endpoints['in_sync'].keys()) > 0:
                trace.warning(f"cluster_tablesync_mgr - detected all endpoints for {cluster} {table_name} are out_of_sync")
                await table_sync_recovery(cluster, table_name, **kw)
                endpoints = await get_table_endpoints(cluster,table_name, caller='tablesync_mgr', trace=kw['trace'])
            endpoints = endpoints['out_of_sync']
            for endpoint in endpoints:
                endpoint_path = endpoints[endpoint]['path']
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
            if cluster == pyql:
                order = ['state','tables','clusters', 'auth', 'endpoints', 'databases', 'jobs', 'transactions']
                jobs_to_run_ordered = []
                ready_jobs = []
                while len(order) > 0:
                    #stateCheck = False
                    last_pop = None
                    for job in jobs[cluster]:
                        if len(order) == 0:
                            break
                        if job['table'] == order[0]:
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
                    await jobs_add(job, trace=kw['trace'])
        trace.info(f"cluster_tablesync_mgr created {jobs} for outofSync endpoints")
        return {"jobs": jobs}
    server.clusterjobs['tablesync_mgr'] = tablesync_mgr

    
    #@server.trace
    #def table_copy(cluster, table, in_sync_path, in_sync_token, in_sync_uuid,  out_of_sync_path, out_of_sync_token, out_of_sync_uuid, **kw):
    @server.trace
    async def table_copy(cluster, table, out_of_sync_path, out_of_sync_token, out_of_sync_uuid, **kw):
        trace=kw['trace']
        pyql = await server.env['PYQL_UUID']
        in_sync_table_copy = await table_select(cluster, table, method='GET', **kw)

        # This allows logs to generate for endpoint - following the copy
        await table_endpoint(cluster, table, out_of_sync_uuid, {'state': 'loaded'}, trace=kw['trace'])
        if 'unPauseAfterCopy' in kw:
            # unpause to allow txn logs to generate while syncing
            r = await table_pause(cluster, table, 'stop')

        response, rc = await probe(
            f'{out_of_sync_path}/sync',
            method='POST', 
            data=in_sync_table_copy, 
            token=out_of_sync_token,
            timeout=None, # liveness has already been checked  
            session=await get_endpoint_sessions(out_of_sync_uuid), **kw)
        if rc == 400 and 'message' in response:
            if 'not found in database' in response['message']:
                # create table & retry resync
                trace.warning(f"table {table} was not found, attempting to create")
                tb_config = table_config(cluster, table)
                response, rc = await probe(
                    f'{out_of_sync_path}/create', 'POST', tb_config, 
                    token=out_of_sync_token, session=await get_endpoint_sessions(out_of_sync_uuid, **kw),  
                    trace=kw['trace'])
                if not rc == 200:
                    response, rc = trace.error(f"failed to create table using {tb_config}"), 500
                #Retry sync since new table creation
                response, rc = await probe(
                    f'{out_of_sync_path}/sync', 'POST', in_sync_table_copy, 
                    token=out_of_sync_token, session=await get_endpoint_sessions(out_of_sync_uuid, **kw),
                    trace=kw['trace'])

        # mark table endpoint as 'new'
        if not rc == 200:
            await table_endpoint(cluster, table, out_of_sync_uuid, {'state': 'new'}, trace=kw['trace'])
        trace.warning(f"#SYNC table_copy results {response} {rc}")
        trace.warning(f"#SYNC initial table copy of {table} in cluster {cluster} completed, need to sync changes now")
        return response
    
    @server.trace
    async def table_sync_run(cluster=None, table=None, config=None, job=None, **kw):
        trace=kw['trace']
        sync_results = {}
        class tracker:
            def __init__(self):
                self.step = 0
            def incr(self):
                self.step+=1
        if not config == None:
            cluster, table, job = config['cluster'], config['table'], config['job']
        if cluster == None or table == None or job==None:
            server.http_exception(
                400, 
                trace.error(
                    f"missing or invalid configuration provided: cluster {cluster} table {config} job: {job} config: {config}"
                )
            )
        pyql_sync_exclusions = {'transactions', 'jobs', 'state', 'tables'}
        pyql = await server.env['PYQL_UUID']
        # get table endpoints
        table_endpoints = await get_table_endpoints(cluster, table, caller='cluster_table_sync_run', trace=kw['trace'])
        trace(f"table endpoints {table_endpoints}")
        if len(table_endpoints['in_sync']) == 0:
            trace(f"no in_sync endpoints - running table_sync_recovery")
            await table_sync_recovery(cluster, table, **kw)
        for endpoint in table_endpoints['out_of_sync']:
            step = tracker()
            def track(message):
                trace.warning(f"tablesyncer {job} cluster {cluster} table {table} endpoint {endpoint} seq={step.step} {message}")
                step.incr()
                return message
            # out_of_sync endpoint to sync
            ep = table_endpoints['out_of_sync'][endpoint]

            uuid, path, token, db, table_state = ep['uuid'], ep['path'], ep['token'], ep['db_name'], ep['state']
            cluster_id = ep['cluster']
            endpoint_path = f"http://{path}/db/{db}/table/{table}"

            # in_sync endpoint To sync against
            in_sync = list(table_endpoints['in_sync'].keys())[random.randrange(len([k for k in table_endpoints['in_sync']]))]
            in_sync_endpoint = table_endpoints['in_sync'][in_sync] 
            in_sync_path =  f"http://{in_sync_endpoint['path']}/db/{db}/table/{table}"
            in_sync_uuid = in_sync_endpoint['uuid']
            in_sync_token = in_sync_endpoint['token']

            # check if endpoint is alive
            r, rc = await probe(f'http://{path}/pyql/node', trace=kw['trace'], session=await get_endpoint_sessions(uuid, **kw))
            if not rc == 200 and not rc==404:
                warning = f"endpoint {uuid} is not alive or reachable with path {path} - cannot issue sync right now"
                sync_results[endpoint] = track(warning)
                continue

            async def load_table():
                track("load_table starting - pausing table to get a consistent table_copy")
                r = await table_pause(cluster, table, 'start', delay_after_pause=4.0)
                if cluster == pyql and table in pyql_sync_exclusions: 
                    #need to blackout changes to these tables during entire copy as txn logs not generated
                    try:
                        track(f"cutover start result: {r}")
                        track(f"starting table_copy")
                        tb_copy_result = await table_copy(cluster, table, endpoint_path, token, uuid, **kw)
                        track(f"table_copy result: {tb_copy_result}")
                        
                        track(f"PYQL - Marking table endpoint as in_sync & loaded")
                        r = await table_endpoint(cluster, table, uuid, {'in_sync': True, 'state': 'loaded'}, trace=kw['trace'])
                        track(f'PYQL - marking table endpoint {uuid} - result: {r}')
                        if cluster == pyql and table == 'state':
                            # as sync endpoint is pyql - state, need to manually set in_sync True on itself
                            status, rc = await probe(
                                f'{endpoint_path}/update',
                                method='POST', 
                                data={'set': {
                                    'state': 'loaded', 'in_sync': True},
                                    'where': {'uuid': endpoint, 'table_name': 'state'}
                                },
                                token=token,
                                session=await get_endpoint_sessions(uuid, **kw),
                                trace=kw['trace']
                            )
                    except Exception as e:
                        trace.exception(track(f"PYQL - exception during load table - {repr(e)}"))
                    r = await table_pause(cluster, table, 'stop', trace=kw['trace'])
                    track(f'PYQL - end of cutover, resuming table result: {r}')
                else: 
                    tb_copy_result = await table_copy(cluster, table, endpoint_path, token, uuid, unPauseAfterCopy=True, **kw)
                    if tb_copy_result:
                        track(f"table_copy results: {tb_copy_result}")
                    else:
                        await table_pause(cluster, table, 'stop')
                        return track(f"table create failed - error {tb_copy_result}"), 500
                return track("load_table completed"), 200
            #
            async def sync_cluster_table_logs():
                try_count = 0
                track('starting sync_cluster_table_logs')
                while True:
                    try:
                        logs_to_sync = await table_endpoint_logs(cluster, table, uuid, 'getAll', trace=kw['trace'])
                        break
                    except Exception as e:
                        try_count+=1
                        if try_count <= 2:
                            track(f"Encountered exception trying to to pull tablelogs, retry # {try_count}")
                            continue
                        error = track(f"error when pulling logs - {repr(e)}")
                        return {"error": trace.exception(error)}, 500
                commited_logs = []
                txns = sorted(logs_to_sync['data'], key=lambda txn: txn['timestamp'])
                track(f"logs to process - count {len(txns)} - {[txn['txn'] for txn in txns]}")
                for txn in txns:
                    transaction = txn['txn']
                    for action in transaction:
                        message, rc = await probe(
                            f'{endpoint_path}/{action}', 'POST', 
                            transaction[action], token=token, 
                            session=await get_endpoint_sessions(uuid), **kw)
                        if rc == 200:
                            commited_logs.append(txn['uuid'])
                        else:
                            track(f"#CRITICAL sync_cluster_table_logs - should not have happened, commiting logs for {uuid} {message} {rc}")
                # confirm txns are applied & remove from txns table
                #/cluster/{cluster}/tablelogs/{table}/{endpoint}/commit - POST
                if len(commited_logs) > 0:
                    commit_result = await commit_table_endpoint_logs(cluster, table, uuid, {'txns': commited_logs}, trace=kw['trace'])
                message = f"sync_cluster_table_logs completed for {cluster} {table}"
                track(message)
                return {"message": message}, 200
            
            # 
            if table_state == 'new':
                track("table never loaded or has become stale, needs to be initialize")
                # delete any txn logs which exist for endpoint
                await post_request_tables(
                    pyql, 'transactions', 
                    'delete', 
                    {'where': {'endpoint': uuid, 'table_name': table}}, **kw)
                result, rc = await load_table()
                track(f"load table results {result} {rc}")
                if not rc == 200:
                    sync_results[endpoint] = result
                    continue
            else:
                # Check for un-commited logs - otherwise full resync needs to occur.
                track("table already loaded, checking for change logs")
                count = await table_endpoint_logs(cluster, table, uuid, 'count', trace=kw['trace'])
                if count:
                    if count['availableTxns'] == 0:
                        track("no change logs found for table, need to reload table - drop / load")
                        # Need to reload table - drop / load
                        result, rc = await load_table()
                        if not rc == 200:
                            sync_results[endpoint] = result
                            continue
                        
            track("trying to sync from change logs")
            await sync_cluster_table_logs()

            if cluster == pyql and table in pyql_sync_exclusions:
                pass
            else:
                track("completed initial pull of change logs & starting a cutover by pausing table")
                r = await table_pause(cluster, table, 'start', trace=kw['trace'], delay_after_pause=4.0)
                #message, rc = table_cutover(cluster_id, table, 'start')
                track(f"cutover result: {r}")
                
                try:
                    track("starting post-cutover pull of change logs")
                    await sync_cluster_table_logs()
                    track("finished post-cutover pull of change logs")
                except Exception as e:
                    trace.exception("sync_cluster_table_logs encountered an exception")
                    track("exception encountered during pull of change logs, aborting cutover")
                    r = await table_pause(cluster, table, 'stop', trace=kw['trace'])
                    return {"error": track(f"exception encountered during pull of change logs")}
                track("setting TB endpoint as in_sync=True, 'state': 'loaded'")
                r = await table_endpoint(cluster, table, uuid, {'in_sync': True, 'state': 'loaded'}, trace=kw['trace'])
                track(f"setting TB endpoint as in_sync=True, 'state': 'loaded' result: {r}")
                # Un-Pause
                track("completing cutover by un-pausing table")
                r = await table_pause(cluster, table, 'stop', trace=kw['trace'])
                track(f"completing cutover result: {r}")
            sync_results[endpoint] = track(f"finished syncing {uuid} for table {table} in cluster {cluster}")
            if cluster == pyql:
                ready = True
                for table_state in await server.clusters.state.select('in_sync', where={"uuid": uuid}):
                    if table_state['in_sync'] == False:
                        ready=False
                # no tables for endpoint are in_sync false - mark endpoint ready = True
                if ready:
                    await update_cluster_ready(path=path, ready=True)
        message = trace(f"finished syncing cluster {cluster} table {table} - results: {sync_results}")
        return {"message": message, "results": sync_results}
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
        pyql = await server.env['PYQL_UUID']
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
                server.http_exception(
                    400, 
                    trace(f'job {job} not added, could not verify if job exists in table, try again later'
                    )
                )
            job_check= job_check['data']

            if len(job_check) > 0:
                job_status = f"job {job_check[0]['id'] }with name {job['job']} already exists"
                trace.warning(job_status)
                return {
                    'message': job_status,
                    'job_id': job['job']
                }

        response = await post_request_tables(pyql, 'jobs', 'insert', job_insert, trace=kw['trace'])
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
        trace = kw['trace']
        # try to pull job 
        job = await jobqueue(job_type, node_id, **kw)
        trace(f"cluster_job_check_and_run pulled: {job}")
        if not job or 'message' in job:
            server.http_exception(200, job)
        trace(f"job pulled {job['name']}")
        job_config = job['config']

        await job_update(job_type, job['id'], 'running', job_info={"message": f"starting {job['name']}"}, **kw)

        trace(f"running job with config {job_config}")
        result = await server.clusterjobs[job['action']](config=job_config, **kw)
        
        if result:
            await job_update(
                job_type, job['id'], 'finished', 
                job_info={
                    "message": f"finished {job['name']}",
                    "result": result
                    }, **kw)
        else:
            error = trace(f"non - 200 result for job {job['name']} config: {job_config} with result: {result}")
            await job_update(job_type, job['id'], 'queued', job_info={"error": f"{error} - requeuing"}, **kw)
            return result
        if 'nextJob' in job_config:
            await job_update(job_type, job_config['nextJob'], 'queued', job_info={"message": f"queued after {job['name']} completed"}, **kw)
        trace(f"finished {job['name']} with result: {result}")
        return result
        
    await server.internal_job_add(join_cluster_job)

    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        #Job to trigger cluster_quorum()
        init_quorum = {
            "job": "init_quorum",
            "job_type": "cluster",
            "method": "POST",
            "action": 'cluster_quorum_update',
            "path": "/pyql/quorum",
            "config": {}
        }
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
            await server.internal_job_add(init_quorum)
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
        

    # Check for number of endpoints in pyql cluster, if == 1, mark ready=True
    quorum = await server.clusters.quorum.select('*')
    # clear existing quorum
    for node in quorum:
        await server.clusters.quorum.delete(where={'node': node['node']})

    if len(endpoints) == 1 or os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        ready_and_quorum = True
        health = 'healthy'
    else:
        await server.clusters.state.update(in_sync=False, where={'uuid': node_id}) 
        ready_and_quorum = False
        health = 'unhealthy'
    # Sets ready false for any node with may be restarting as resync is required before marked ready

    await server.clusters.quorum.insert(**{
        'node': node_id,
        'nodes': {'nodes': [node_id]},
        'missing': {},
        'in_quorum': ready_and_quorum,
        'health': health,
        'last_update_time': float(time.time()),
        'ready': ready_and_quorum,
    })