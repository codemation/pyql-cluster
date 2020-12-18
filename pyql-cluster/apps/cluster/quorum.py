async def run(server):
    import os, uuid, time
    import asyncio
    from fastapi import Request, Depends

    log = server.log

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

            node_quorum = await server.data['cluster'].tables['quorum'].select(
                '*',
                where={
                    'node': server.PYQL_NODE_ID
                }
            )
            state = await server.data['cluster'].tables['state'].select(
                'state',
                where={
                    'name': f"{server.PYQL_NODE_ID}_state"
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
                pyql_nodes = await server.data['cluster'].tables['endpoints'].select('uuid', 'path', where={'cluster': pyql})
                headers = dict(request.headers)
                # pop header fields which should not be passed
                for h in ['Content-Length']:
                    if h.lower() in headers:
                        headers.pop(h.lower())
                    if h in headers:
                        headers.pop(h)
                if not 'unsafe' in headers:
                    headers['unsafe'] = [server.PYQL_NODE_ID]
                else:
                    headers['unsafe'] = headers['unsafe'].split(',') + [server.PYQL_NODE_ID]
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
                'set': {'ready': ready['ready']}, 'where': {'node': server.PYQL_NODE_ID}
            }
            await server.data['cluster'].tables['quorum'].update(
                **update_set['set'], where=update_set['where'])
            return ready
    @server.trace
    async def update_cluster_ready(path=None, ready=None, config=None, **kw):
        if not config == None:
            path, ready = config['path'], config['ready']
        return await server.probe(f"http://{path}/cluster/pyql/ready", method='POST', data={'ready': ready}, **kw)

    server.clusterjobs['update_cluster_ready'] = update_cluster_ready
    server.update_cluster_ready = update_cluster_ready


    @server.api_route('/pyql/quorum/check', methods=['POST'])
    async def cluster_quorum_refresh(request: Request, token: dict = Depends(server.verify_token)):
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
        pyql_endpoints = await server.data['cluster'].tables['endpoints'].select('*', where={'cluster': pyql})
        if len(pyql_endpoints) == 0:
            return {
                "message": trace.warning(
                    "cluster_quorum_check found no pyql_endpoints, cluster may still be initializing")
                    }
        quorum = await server.data['cluster'].tables['quorum'].select('*')
        # Check which pyql_endpoints are alive   
        alive_endpoints = await server.get_alive_endpoints(pyql_endpoints, **kw)
        alive_endpoints_nodes = [server.PYQL_NODE_ID]
        for endpoint in alive_endpoints:
            alive_endpoints_nodes.append(endpoint)
        # Compare live endpoints to current quorum 
        latestQuorumNodes = quorum[0]['nodes']
        if len(alive_endpoints_nodes) / len(pyql_endpoints) < 2/3: 
            quorum = {'alive': alive_endpoints_nodes, 'members': pyql_endpoints}
            server.http_exception(
                500, 
                trace.warning(f" detected node {server.PYQL_NODE_ID} is outOfQuorum - quorum {quorum}"))


        ep_requests = {}
        ep_list = []
        quorum_updates = [] # cluster_quorum_update

        for endpoint in pyql_endpoints:
            # only trigger a pyql/quorum update on live endpoints
            if endpoint['uuid'] in alive_endpoints_nodes:
                quorum_updates.append(
                    server.rpc_endpoints[endpoint['uuid']]['cluster_quorum_update']()
                )
                ep_list.append(endpoint['uuid'])

        trace.warning(f"cluster_quorum_check - running using {ep_requests}")
        if len(ep_list) == 0:
            return {"message": f"pyql node {server.PYQL_NODE_ID} is still syncing"}

        results = await asyncio.gather(*quorum_updates, return_exceptions=True) 

        trace.warning(f"cluster_quorum_check - results {results}")

        return {
            "message": trace(f"cluster_quorum_check completed on {server.PYQL_NODE_ID}"), 
            'results': results
            }
    server.clusterjobs['cluster_quorum_check'] = cluster_quorum_check
    server.cluster_quorum_check = cluster_quorum_check

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    @server.trace
    async def cluster_quorum_update(**kw):
        trace = kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        pyql_endpoints = await server.data['cluster'].tables['endpoints'].select(
            '*', 
            where={'cluster': pyql}
        )

        if len(pyql_endpoints) == 0:
            return {"message": trace(f"cluster_quorum_update node {server.PYQL_NODE_ID} is still syncing")}
        if len(pyql_endpoints) == 1:
            health = 'healthy'
        # get last recorded quorum - to check against
        last_quorum = await server.data['cluster'].tables['quorum'].select(
            '*', 
            where={'node': server.PYQL_NODE_ID}
        )
        last_quorum = last_quorum[0]
        trace(f"last_quorum  - {last_quorum}")

        # check alive endpoints
        alive_endpoints = await server.get_alive_endpoints(pyql_endpoints, **kw)
        trace(f"alive_endpoints - {alive_endpoints}")

        # build list of in_quorum & missing nodes

        in_quorum_nodes, missing_nodes = {}, {}
        for endpoint in pyql_endpoints:
            ep_uuid = endpoint['uuid']
            if ep_uuid in alive_endpoints:
                in_quorum_nodes[ep_uuid] = time.time()
            else:
                if not ep_uuid in last_quorum['missing']:
                    missing_nodes[ep_uuid] = time.time()
                else:
                    missing_nodes[ep_uuid] = last_quorum['missing'][ep_uuid]

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
                    'job': f"{server.PYQL_NODE_ID}_mark_state_stale",
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
                                    'node': server.PYQL_NODE_ID
                                }
                            },
                            'where': {
                                'uuid': server.PYQL_NODE_ID
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
                await server.data['cluster'].tables['state'].update(
                    loaded='stale',
                    info={
                        'stale reason': 'endpoint became un-healthy',
                        'operation': trace.get_root_operation(),
                        'node': server.PYQL_NODE_ID
                    },
                    where={
                        'uuid': server.PYQL_NODE_ID
                    }
                )
        trace(f"quorum_to_set - {quorum_to_set}")
        await server.data['cluster'].tables['quorum'].update(
            **quorum_to_set,
            where={
                'node': server.PYQL_NODE_ID
            }
        )
        updated_quorum = await server.data['cluster'].tables['quorum'].select(
            '*',
            where={'node': server.PYQL_NODE_ID}
        )
        trace(f"current_quorum - {updated_quorum}")
        return {"quorum": updated_quorum[0]}
    server.clusterjobs['cluster_quorum_update'] = cluster_quorum_update

        
    @server.api_route('/pyql/quorum', methods=['GET', 'POST'])
    async def cluster_quorum_query_api(request: Request, token: dict = Depends(server.verify_token)):
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
        quorum = await server.data['cluster'].tables['quorum'].select(
            '*', where={'node': server.PYQL_NODE_ID})
        return {'quorum': quorum[0]}
    server.cluster_quorum_query = cluster_quorum_query



    @server.trace
    async def cluster_quorum(update=False, **kw):
        trace = kw['trace']
        if update == True:
            await cluster_quorum_update(trace=trace)
        quorum_select = await server.data['cluster'].tables['quorum'].select('*', where={'node': server.PYQL_NODE_ID})
        return {'quorum': quorum_select[0]}