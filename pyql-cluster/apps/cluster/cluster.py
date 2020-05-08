"""
App for handling pyql-endpoint cluster requests
#TODO - consider reducing stuck job detection time window or implement a call-back so can more quickly cleanup a stuck job
#TODO - with cluster pqyl is currently possible for 1 node to remove an endpoint ( due to outOfQuorum ) but other nodes may not have failed
        resulting in 1 endpoint which is more vulnerable to a 2/3's quorum failure. Need to identify these cases & add back if other nodes report
        less inQuorum nodes than what was just found. 
"""
def run(server):
    from flask import request
    import requests
    from datetime import datetime
    import time, uuid, random
    from random import randrange
    import json, os
    from apps.cluster import asyncrequest

    log = server.log

    class tracer:
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
            rootDuration = self.get_root_caller_duration()
            localDuration = time.time() - self.start
            return f"{self.get_callers()} - {rootDuration:.3f} - {localDuration:.3f}s - {message}"

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
            funcName = f'{func}'.split(' ')[1].split('.')[-1]
            if not 'trace' in kwargs:
                kwargs['trace'] = tracer(funcName)
            else:
                kwargs['trace'] = tracer(funcName, kwargs['trace'])
            return func(*args, **kwargs)
        return traced
    server.trace = trace

    class cluster:
        """
            object used for quickly referencing tables
            clusters, endpoints, databases, tables, state
        """
        def __init__(self, dbname):
            for table in server.data[dbname].tables:
                setattr(self, table, server.data[dbname].tables[table])

    server.clusters = cluster('cluster')

    endpoints = server.clusters.endpoints.select('*') if 'endpoints' in server.data['cluster'].tables else []

    uuidCheck = server.data['cluster'].tables['pyql'].select('uuid', where={'database': 'cluster'})
    if len(uuidCheck) > 0:
        for _,v in uuidCheck[0].items():
            dbuuid = str(v)
    else:
        dbuuid = str(uuid.uuid1())
        server.data['cluster'].tables['pyql'].insert({
            'uuid': dbuuid,
            'database': 'cluster', 
            'lastModTime': time.time()
        })
    nodeId = dbuuid
    os.environ['PYQL_ENDPOINT'] = dbuuid
    server.env['PYQL_ENDPOINT'] = dbuuid

    os.environ['HOSTNAME'] = '-'.join(os.environ['PYQL_NODE'].split('.'))

    if not 'PYQL_CLUSTER_ACTION' in os.environ:
        os.environ['PYQL_CLUSTER_ACTION'] = 'join'

    # Table created only if 'init' is passed into os.environ['PYQL_CLUSTER_ACTION']
    tables = []
    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        tableList = ['clusters', 'endpoints', 'tables', 'state', 'transactions', 'jobs', 'auth']
        tables = [{tableName: server.get_table_func('cluster', tableName)[0]} for tableName in tableList]
    
    joinJobType = "node" if os.environ['PYQL_CLUSTER_ACTION'] == 'init' or len(endpoints) == 1 else 'cluster'

    joinClusterJob = {
        "job": f"{os.environ['HOSTNAME']}{os.environ['PYQL_CLUSTER_ACTION']}Cluster",
        "jobType": joinJobType,
        "method": "POST",
        "path": "/cluster/pyql/join",
        "data": {
            "name": os.environ['HOSTNAME'],
            "path": f"{os.environ['PYQL_NODE']}:{os.environ['PYQL_PORT']}",
            "token": server.env['PYQL_LOCAL_SERVICE_TOKEN'],
            "database": {
                'name': "cluster",
                'uuid': dbuuid
            },
            "tables": tables
        }
    }
    if 'PYQL_CLUSTER_JOIN_TOKEN' in os.environ and os.environ['PYQL_CLUSTER_ACTION'] == 'join':
        joinClusterJob['joinToken'] = os.environ['PYQL_CLUSTER_JOIN_TOKEN']

    def get_clusterid_by_name_authorized(clusterName, **kwargs):
        userId = request.auth
        request.clusterName = clusterName
        log.warning(f"check_user_access called for cluster {clusterName} using {userId}")
        clusters = server.clusters.clusters.select('*', where={'name': clusterName})
        clusterAllowed = None
        for cluster in clusters: 
            if userId == cluster['owner'] or userId in cluster['access']['allow']:
                clusterAllowed = cluster['id']
                break
            if 'authChildren' in request.__dict__:
                for childId in request.authChildren:
                    if childId == cluster['owner'] or childId in cluster['access']['allow']:
                        clusterAllowed = cluster['id']
                        break
        if clusterAllowed == None:
            env = kwargs
            warning = f"user {userId} access to cluster with name {clusterName}, no cluster was found which user has access rights or none exists - env {env}"
            return {"warning": log.warning(warning)}, 404
        return str(clusterAllowed), 200
    server.get_clusterid_by_name_authorized = get_clusterid_by_name_authorized

    @server.trace
    def get_auth_http_headers(location=None, token=None, **kw):
        trace=kw['trace']
        if token == None:
            auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not location == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
            trace.warning(f"get_auth_http_headers called using location: {location} - token: {token} - {kw} auth: {auth}")
            token = kw['token'] if 'token' in kw else None
            token = server.env[auth] if token == None else token
        headers = {
            'Accept': 'application/json', "Content-Type": "application/json",
            "Authentication": f"Token {token}"}
        trace.warning(f"get_auth_http_headers {headers}")
        return headers


    def cluster_name_to_uuid(func):
        """
        After authenticaion, uses 'userid' in server.auth to check
        if user has "access" to a cluster with name in kw['cluster']
        and replaces string name with 'uuid' of cluster if exists
        """
        def check_user_access(*args, **kwargs):
            if not 'auth' in request.__dict__:
                log.error("authentication is required or missing, this should have been handled by is_authenticated")
                return {"error": "authentication is required or missing"}, 500
            """
            ('id', str, 'UNIQUE NOT NULL'),
            ('name', str),
            ('owner', str), # UUID of auth user who created cluster 
            ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            ('createdByEndpoint', str),
            ('createDate', str)
            """
            clusterName = kwargs['cluster']
            kwargs['cluster'], rc = get_clusterid_by_name_authorized(
                kwargs['cluster'], func=func, kwargs=kwargs, args=args, auth=request.auth)
            if not rc == 200:
                return kwargs['cluster'], rc
            #kwargs['cluster'] = clusterAllowed
            request.clusterName = clusterName
            # TODO decide if i need to input clusterName into kwargs
            return func(*args, **kwargs)
        return check_user_access

    @server.route('/pyql/setup', methods=['POST'])
    @server.is_authenticated('local')
    def cluster_set_pyql_id():
        return set_pyql_id()
    
    def set_pyql_id(pyqlId=None):
        pyqlId = request.get_json()['PYQL_UUID'] if pyqlId == None else pyqlId
        server.env['PYQL_UUID'] = pyqlId
        return {'message': "updated"}, 200

    @server.route('/cache/reset', methods=['POST'])
    @server.is_authenticated('local')
    @server.trace
    def cluster_node_reset_cache(reason=None, **kw):
        node_reset_cache(trace=kw['trace'])

    @server.trace
    def node_reset_cache(reason=None, **kw):
        """
            resets local db table 'cache' 
        """
        trace=kw['trace']
        reason = request.get_json() if reason == None else reason
        trace(f"cache reset called for {reason}")
        server.reset_cache()
    server.node_reset_cache = node_reset_cache

    @server.trace
    def probe(path, method='GET', data=None, timeout=3.0, auth=None, **kw):
        trace = kw['trace']
        auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
        headers = get_auth_http_headers(auth, **kw)
        url = f'{path}'
        try:
            if method == 'GET':
                r = requests.get(url, headers=headers, timeout=2.0)
            else:
                r = requests.post(url, headers=headers, data=json.dumps(data), timeout=timeout)
        except Exception as e:
            error = f"Encountered exception when probing {path} - {repr(e)}"
            return {"error": trace.error(error)}, 500
        try:
            return r.json(),r.status_code
        except Exception as e:
            trace.error(f'encountered exception - {repr(e)}')
            return r.text, r.status_code
    server.probe = probe
    def wait_on_jobs(pyql, curInd, jobList, waitingOn=None):
        """
            job queing helper function - guarantees 1 job runs after the other by creating "waiting jobs" 
             dependent on the first job completing
        """
        if len(jobList) > curInd + 1:
            jobList[curInd]['nextJob'] = wait_on_jobs(pyql, curInd+1, jobList)
        if curInd == 0:
            return jobs_add('syncjobs' if jobList[curInd]['jobType'] == 'tablesync' else 'jobs', 
                jobList[curInd])[0]['jobId']
        return jobs_add('syncjobs' if jobList[curInd]['jobType'] == 'tablesync' else 'jobs', 
            jobList[curInd], status='waiting')[0]['jobId']
    # TODO - Delete is_requests_success
    def is_requests_success(r, module):
        """
            logs error if non-200 rc, and returns false
        """
        if isinstance(r, requests.models.Response):
            if not r.status_code == 200:
                log.error(f"{module} encountered an error with requests {r.text} {r.status_code}")
                return False
            else:
                return True
    @server.trace
    def bootstrap_pyql_cluster(config, **kw):
        """
            runs if this node is targeted by /cluster/pyql/join and pyql cluster does not yet exist
        """
        trace = kw['trace']
        trace.info(f"bootstrap starting for {config['name']} config: {config}")
        def get_clusters_data():
            # ('id', str, 'UNIQUE NOT NULL'),
            # ('name', str),
            # ('owner', str), # UUID of auth user who created cluster 
            # ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
            # ('createdByEndpoint', str),
            # ('createDate', str)
            return {
                'id': str(uuid.uuid1()),
                'name': 'pyql',
                'owner': request.auth,
                'access': {'allow': [request.auth]},
                'key': server.encode(
                    os.environ['PYQL_CLUSTER_INIT_ADMIN_PW'],
                    key=server.env['PYQL_CLUSTER_TOKEN_KEY']
                    ),
                'createdByEndpoint': config['name'],
                'createDate': f'{datetime.now().date()}'
                }
        def get_endpoints_data(clusterId):
            return {
                'uuid': config['database']['uuid'],
                'dbname': config['database']['name'],
                'path': config['path'],
                'token': config['token'],
                'cluster': clusterId
            }
        def get_databases_data(clusterId):
            return {
                'name': f'{config["name"]}_{config["database"]["name"]}',
                'cluster': clusterId,
                'uuid': config['database']['uuid'],
                'dbname': config['database']['name'],
                'endpoint': config['name']
            }
        def get_tables_data(table,clusterId, cfg):
            return {
                'name': table,
                'cluster': clusterId,
                'config': cfg,
                'isPaused': False
            }
        def get_state_data(table, clusterId):
            return {
                'name': f'{config["database"]["uuid"]}{table}',
                'state': 'loaded',
                'inSync': True,
                'tableName': table,
                'cluster': clusterId,
                'uuid': config['database']['uuid'], # used for syncing logs 
                'lastModTime': time.time()
            }
        def execute_request(endpoint, db, table, action, data):
            r = requests.post(
                        f'{endpoint}/db/{db}/table/{table}/{action}',
                        headers={
                            'Accept': 'application/json', 
                            "Content-Type": "application/json",
                            "Authentication": f"Token {server.env['PYQL_LOCAL_SERVICE_TOKEN']}"
                            },
                        data=json.dumps(data),
                        timeout=0.5)
            is_requests_success(r,'bootstrap_pyql_cluster')
            return r
        localhost = f'http://localhost:{os.environ["PYQL_PORT"]}'
        # cluster table
        clusterData = get_clusters_data()
        execute_request(localhost, 'cluster', 'clusters',
            'insert', clusterData)
        # endpoints
        execute_request(localhost, 'cluster', 'endpoints',
            'insert', get_endpoints_data(clusterData['id']))
        # tables & state 
        for table in config['tables']:
            for tableName, cfg in table.items():
                r = execute_request(
                    localhost,
                    'cluster',
                    'tables',
                    'insert',
                    get_tables_data(tableName,clusterData['id'], cfg)
                )
        for table in config['tables']:
            for name, cfg in table.items():
                r = execute_request(
                    localhost,  
                    'cluster',
                    'state',
                    'insert',
                    get_state_data(name, clusterData['id'])
                )
        trace.info("finished bootstrap")
    
    #No auth should be required 
    @server.route('/pyql/node')
    def cluster_node():
        """
            returns node-id - to be used by workers instead of relying on pod ip:
        """
        log.warning(f"get nodeId called {nodeId}")
        return {"uuid": nodeId}, 200

    #TODO - Need to determine if this is used & delete
    @server.route('/cluster/pyql/state/<action>', methods=['GET','POST'])
    def cluster_state(action):
        endpoints = get_table_endpoints('pyql', 'state', caller='cluster_state')['inSync']
        if request.method == 'GET':
            pass
        if request.method == 'POST':
            if action == 'sync':
                pass
    
    @server.route('/cluster/pyql/ready', methods=['POST', 'GET'])
    @server.is_authenticated('cluster')
    @server.trace
    def cluster_ready(ready=None, **kw):
        trace = kw['trace']
        if request.method == 'GET':
            #quorum, rc = cluster_quorum(True)
            quorum, rc = cluster_quorum_update(trace=kw['trace'])
            trace.warning(f"readycheck - {quorum}")
            if "quorum" in quorum and quorum['quorum']["ready"] == True:
                return quorum['quorum'], 200
            else:
                return quorum, 400
        else:
            """
                expects:
                ready =  {'ready': True|False}
            """
            ready = request.get_json() if ready == None else ready
            ready = ready['data'] if 'data' in ready else ready
            updateSet = {
                'set': {'ready': ready['ready']}, 'where': {'node': nodeId}
            }
            server.clusters.quorum.update(**updateSet['set'], where=updateSet['where'])
            return ready, 200
    @server.trace
    def cluster_endpoint_delete(cluster, endpoint, **kw):
        trace = kw['trace']
        trace.error(f"cluster_endpoint_delete called for cluster - {cluster}, endpoint - {endpoint}")
        deleteWhere = {'where': {'uuid': endpoint, 'cluster': cluster}}
        server.clusters.state.delete(**deleteWhere)
        server.clusters.endpoints.delete(**deleteWhere)
        return {"message": trace(f"deleted {endpoint} successfully")}, 200

    @server.trace
    def get_alive_endpoints(endpoints, timeout=2.0, **kw):
        trace = kw['trace']
        epRequests = {}
        for endpoint in endpoints:
            if endpoint['uuid'] == nodeId:
                # no need to check own /pyql/node 
                continue 
            epRequests[endpoint['uuid']] = {
                'path': f"http://{endpoint['path']}/pyql/node",
                'timeout':timeout,
            }
        try:
            epResults = asyncrequest.async_request(epRequests)
        except Exception as e:
            return {"error": trace.exception(f"Excepton found during get_alive_endpoints")}, 500
        trace.warning(f"get_alive_endpoints - {epResults}")
        return epResults

    @server.route('/pyql/quorum/check', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_quorum_refresh(**kw):
        return cluster_quorum_check(trace=kw['trace'])

    @server.trace
    def cluster_quorum_check(**kw):
        trace = kw['trace']
        pyql = server.env['PYQL_UUID']
        trace.warning(f"received cluster_quorum_check for cluster {pyql}")
        pyqlEndpoints = server.clusters.endpoints.select('*', where={'cluster': pyql})
        if len(pyqlEndpoints) == 0:
            return {"message": trace.warning("cluster_quorum_check found no pyqlEndpoints, cluster may still be initializing")}, 200
        quorum = server.clusters.quorum.select('*')
        # Check which pyqlEndpoints are alive   
        aliveEndpoints = get_alive_endpoints(pyqlEndpoints, trace=trace)
        aliveEndpointsNodes = [nodeId]
        for endpoint in aliveEndpoints:
            if aliveEndpoints[endpoint]['status'] == 200:
                aliveEndpointsNodes.append(endpoint)
        # Compare live endpoints to current quorum 
        latestQuorumNodes = quorum[0]['nodes']['nodes']
        if len(latestQuorumNodes) == len(aliveEndpointsNodes):
            #trace.warning("cluster_quorum_check completed, no detected quorum changes")
            # check each node to ensure quorum 
            return {"message": trace.warning("cluster_quorum_check completed, no detected quorum changes")}, 200
        if len(aliveEndpointsNodes) / len(pyqlEndpoints) < 2/3: 
            quorum = {'alive': aliveEndpointsNodes, 'members': pyqlEndpoints}
            return {"message": trace.warning(f" detected node {nodeId} is outOfQuorum - quorum {quorum}")}, 500

        trace.warning(f"cluster_quorum_check detected quorum change, triggering update on aliveEndpointsNodes {aliveEndpointsNodes}")
        #quorumNodes = {q['node']: q for q in quorum}

        epRequests = {}
        epList = []
        for endpoint in pyqlEndpoints:
            # only trigger a pyql/quorum update on live endpoints
            if endpoint['uuid'] in aliveEndpointsNodes:
                epList.append(endpoint['uuid'])
                endPointPath = endpoint['path']
                endPointPath = f'http://{endPointPath}/pyql/quorum'
                epRequests[endpoint['uuid']] = {
                    'path': endPointPath, 'data': None, 'timeout': 5.0,
                    'headers': get_auth_http_headers('remote', token=endpoint['token'])
                    }

        """ TODO - Delete after testing
        # check if quorum table contains stale endpoints & cleanup
        for endpoint in quorumNodes:
            if not endpoint in epList:
                server.clusters.quorum.delete(where={'node': endpoint})
        """

        trace.warning(f"cluster_quorum_check - running using {epRequests}")
        if len(epList) == 0:
            return {"message": f"pyql node {nodeId} is still syncing"}, 200
        try:
            epResults = asyncrequest.async_request(epRequests, 'POST')
        except Exception as e:
            trace.exception("Excepton found during cluster_quorum() check")
        trace.warning(f"cluster_quorum_check - results {epResults}")

        """ TODO - Delete after testing, this func should not touch quorum tables as this is handled by cluster_quorum
        inQuorum = []
        for endpoint in epResults:
            if not endpoint in quorumNodes:
                # insert node entry into quorum table
                server.clusters.quorum.insert(node=endpoint, lastUpdateTime=float(time.time()))
            if epResults[endpoint]['status'] == 200:
                inQuorum.append(endpoint)
                if endpoint in quorumNodes and not endpoint == nodeId:
                    server.clusters.quorum.update(
                        **{'lastUpdateTime': float(time.time())},
                        where={'node': endpoint}
                    )
        isNodeInQuorum = None
        if float(len(inQuorum) / len(epList)) >= float(2/3):
            isNodeInQuorum = True
        else:
            isNodeInQuorum = False
        server.clusters.quorum.update(
                **{
                    'inQuorum': isNodeInQuorum, 
                    'nodes': {'nodes': inQuorum},
                    'lastUpdateTime': float(time.time())
                }, 
                where={'node': nodeId}
                )
        quorum = server.clusters.quorum.select('*', where={'node': nodeId})[0]
        """
        return {"message": trace(f"cluster_quorum_check completed on {nodeId}"), 'results': epResults }, 200
        

    @server.route('/pyql/quorum', methods=['GET', 'POST'])
    @server.is_authenticated('local')
    @server.trace
    def cluster_quorum_query(check=False, get=False, **kw):
        trace=kw['trace']
        if request.method == 'POST':
            #return cluster_quorum(check, get)
            return cluster_quorum_update(trace=kw['trace'])
        return {'quorum': server.clusters.quorum.select('*', where={'node': nodeId})}, 200

    @server.trace
    def cluster_quorum_update(**kw):
        trace = kw['trace']
        pyql = server.env['PYQL_UUID']
        endpoints = server.clusters.endpoints.select('*', where={'cluster': pyql})
        if len(endpoints) == 0:
            # may be a new node / still syncing
            return {"message": trace(f"cluster_quorum_update node {nodeId} is still syncing")}, 200
        """
        epRequests = {}
        for endpoint in endpoints:
            epRequests[endpoint['uuid']] = {
                'path': f"http://{endpoint['path']}/pyql/node",
                'timeout': 0.5,
            }
        try:
            epResults = asyncrequest.async_request(epRequests)
        except Exception as e:
            error = log.exception(f"Excepton found during cluster_quorum_update, failed to update - {repr(e)}")
            return {"error": error}, 500
        """
        epResults = get_alive_endpoints(endpoints, trace=trace)
        # Check results
        inQuorumNodes = []
        for endpoint in epResults:
            if epResults[endpoint]['status'] == 200:
                inQuorumNodes.append(endpoint)
        # Quorum always assume local checking node is alive
        if not nodeId in inQuorumNodes:
            inQuorumNodes.append(nodeId)
        inQuorum = False
        if len(inQuorumNodes) / len(endpoints) >= 2/3:
            inQuorum = True
        server.clusters.quorum.update(
            inQuorum=inQuorum, 
            nodes={"nodes": inQuorumNodes},
            lastUpdateTime=float(time.time()),
            where={'node': nodeId}
        )
        return {
            "message": trace(f"cluster_quorum_update on node {nodeId} updated successfully"),
            'quorum': server.clusters.quorum.select('*', where={'node': nodeId})[0]}, 200
    @server.trace
    def cluster_quorum(update=False, **kw):
        trace = kw['trace']
        if update == True:
            cluster_quorum_update(trace=trace)
        return {'quorum': server.clusters.quorum.select('*', where={'node': nodeId})[0]}, 200

    """TODO - Delete after testing in K8s env
    def cluster_quorum_old(check=False, get=False):
        pyql = server.env['PYQL_UUID']
        if request.method == 'POST' or check == True:
            # list of endpoints to verify quorum
            pyqlEndpoints = server.clusters.endpoints.select('*', where={'cluster': pyql})
            if len(pyqlEndpoints) == 0:
                warning = f"{os.environ['HOSTNAME']} - pyql node is still syncing"
                return {"message": log.warning(warning)}, 200
            epRequests = {}
            for endpoint in pyqlEndpoints:
                epRequests[endpoint['uuid']] = {
                    'path': f"http://{endpoint['path']}/pyql/quorum", 
                    'headers': get_auth_http_headers('remote', token=endpoint['token'])
                    }
            if len(epRequests) == 0:
                return {"message": f"pyql node {nodeId} is still syncing"}, 200

            # invokes /pyql/quorum GET on each endpoint 
            try:
                epResults = asyncrequest.async_request(epRequests)
            except Exception as e:
                log.exception("Excepton found during cluster_quorum() check")
            inQuorum, outQuorum = [], []
            log.warning(f"epResults - {epResults}")
            for endpoint in epResults:
                if epResults[endpoint]['status'] == 200 or endpoint == nodeId:
                    inQuorum.append(endpoint)
                else:
                    outQuorum.append(endpoint)
            quorumSet = {}
            isNodeInQuorum = False
            isReady = False
            health = 'unhealthy'
            data = {'set': {'inSync': False}, 'where': {'uuid': nodeId, 'cluster': pyql}}
            if nodeId in epResults and 'quorum' in epResults[nodeId]['content']:
                nodeQuorum = epResults[nodeId]['content']['quorum']
            else:
                nodeQuorum = server.clusters.quorum.select('*', where={'node': nodeId})[0]
            # Check for 2 out of 3 quorum ratio requirement 
            if float(len(inQuorum) / len(epRequests)) >= float(2/3):
                # ready status is dependent on local state table sync status
                stateInSync = server.clusters.state.select(
                    'inSync', 
                    where={'uuid': nodeId, 'tableName': 'state', 'cluster': pyql}
                )[0]['inSync']
                # sync quorum ready status 
                if not nodeQuorum['ready'] == stateInSync:
                    quorumSet['ready'] = stateInSync
                # mark node inQuorum if outOfQuorum
                if not nodeQuorum['inQuorum'] == True:
                    quorumSet['inQuorum'] = True
                isNodeInQuorum = True
                isReady = stateInSync
                # set quorum health to 'healing' & trigger cluster re-join job
                if not isReady and not nodeQuorum['health'] == 'healing':
                    log.error("CRITCAL: need to rejoin cluster as state table has become outOfSync")
                    quorumSet['health'] = 'healing'
                    server.internal_job_add(joinClusterJob)
                    node_reset_cache(f"node {nodeId} is {health}")
                # quorum was not-ready before but should be now - updating health
                if nodeQuorum['ready'] == False and isReady == True and nodeQuorum['health'] == 'healing':
                    quorumSet['health'] = 'healthy'
            else: # Node is outOfQuorum
                if not nodeQuorum['inQuorum'] == False:
                    quorumSet['inQuorum'] = False
                if not nodeQuorum['health'] == 'unhealthy':
                    quorumSet['health'] = 'unhealthy'
                # since node is outOfQuorum, the local state table can no longer be trusted
                server.clusters.state.update(**data['set'], where=data['where'])
                server.internal_job_add(joinClusterJob)
                node_reset_cache(f"node {nodeId} is outOfQuorum")
            if nodeQuorum['nodes'] == None or not nodeQuorum['nodes']['nodes'] == inQuorum:
                quorumSet['nodes'] = {'nodes': inQuorum}
            if len(quorumSet) > 0:
                quorumSet['lastUpdateTime'] = float(time.time())
                server.clusters.quorum.update(**quorumSet, where={'node': nodeId})
            if 'PYQL_TYPE' in os.environ and os.environ['PYQL_TYPE'] == 'K8S':
                if isNodeInQuorum:
                    # remove outOfQuorum endpoint from cluster - cannot always guarantee the same DB will be available / re-join
                    for endpoint in outQuorum:
                        # removal prevents new quorum issues if node is created with a different ID as 2/3 ratio must be maintained
                        cluster_endpoint_delete(pyql, endpoint)
                    # Compary other node quorum results to determine if a node is missing
                    missingNodes = {}
                    for endpoint in epResults:
                        if endpoint == nodeId or endpoint in outQuorum:
                            continue
                        if 'content' in epResults[endpoint] and 'quorum' in epResults[endpoint]['content']:
                            endpointQuorum = epResults[endpoint]['content']['quorum']
                            if not endpointQuorum['nodes'] == None:
                                endpointNodes = endpointQuorum['nodes']['nodes']
                                for node in endpointNodes:
                                    if not node in inQuorum and not node in outQuorum:
                                        if not node in missingNodes:
                                            missingNodes[node] = []
                                        missingNodes[node].append(endpoint)
                    for node in missingNodes:
                        if len(missingNodes[node]) / len(epRequests) >= 2/3:
                            log.warning(f"local endpoint {nodeId} is inQuorum but missing nodes")
                            log.warning(f"marking local endpoint tables inSync False as need to resync")
                            # make job to rejoin cluster
                            server.clusters.state.update(**data['set'], where=data['where'])
                            node_reset_cache(f"node {nodeId} is inQuorum, but missing nodes, need to rejoin cluster and resync")
                            server.internal_job_add(joinClusterJob)
            quorum = server.clusters.quorum.select('*', where={'node': nodeId})[0]
            return {"message": f"quorum updated on {nodeId}", 'quorum': quorum},200
        else:
            try:
                quorum = server.clusters.quorum.select('*', where={'node': nodeId})[0]
            except Exception as e:
                log.exception(f"exception occured during cluster_quorum for {nodeId} {quorum} ")
                quorum = []
            if quorum == None:
                log.error(f"exception occured during cluster_quorum for {nodeId} {quorum} ")
                quorum = []
            return {'message':'OK', 'quorum': quorum}, 200
    """
   
    @server.route('/cluster/<cluster>/table/<table>/path')
    @server.is_authenticated('pyql')
    @server.trace
    def get_db_table_path(cluster, table, **kw):
        paths = {'inSync': {}, 'outOfSync': {}}
        tableEndpoints = get_table_endpoints(cluster, table, caller='get_db_table_path', trace=kw['trace'])
        tb = get_table_info(cluster, table, tableEndpoints, trace=kw['trace'])
        for pType in paths:
            for endpoint in tableEndpoints[pType]:
                #dbName = get_db_name(cluster, endpoint)
                dbName = tb['endpoints'][f'{endpoint}{table}']['dbname']
                paths[pType][endpoint] = tb['endpoints'][f'{endpoint}{table}']['path']
        return paths, 200

    @server.route('/cluster/<cluster>/table/<table>/endpoints')
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_get_table_endpoints(cluster, table, **kw):
        clusterName = request.__dict__.get('clusterName')
        return get_table_endpoints(cluster, table, clusterName, caller='cluster_get_table_endpoints', trace=kw['trace'])

    @server.trace
    def get_table_endpoints(cluster, table, clusterName=None, caller=None, **kw):
        """
        Usage:
            get_table_endpoints('cluster_uuid', 'tableName')
        """
        trace = kw['trace']
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}

        endpoints = server.clusters.endpoints.select(
            '*', 
            join={'state': {'endpoints.uuid': 'state.uuid', 'endpoints.cluster': 'state.cluster'}}, 
            where={'state.cluster': cluster, 'state.tableName': table}
            )
        
        endpointsKeySplit = []
        for endpoint in endpoints:
            renamed = {}
            for k,v in endpoint.items():
                renamed[k.split('.')[1]] = v
            endpointsKeySplit.append(renamed)
        for endpoint in endpointsKeySplit:
            sync = 'inSync' if endpoint['inSync'] == True else 'outOfSync'
            tableEndpoints[sync][endpoint['uuid']] = endpoint
        if not clusterName == None:
            tableEndpoints['clusterName'] = clusterName
        trace.warning(f"{caller} --> get_table_endpoints result {tableEndpoints}")
        return tableEndpoints
        """ TODO - Delete after testing
        endpointsInCluster = server.clusters.endpoints.select(
            '*',
            where={'cluster': cluster}
        ) 
        tablesEndpointState = server.clusters.state.select('*', where={'cluster': cluster, 'tableName': table})
        for endpoint in endpointsInCluster:
            tableEndpoint = f"{endpoint['uuid']}{table}"
            for state in tablesEndpointState:
                if state['name'] == tableEndpoint:
                    sync = 'inSync' if state['inSync'] == True else 'outOfSync'
                    tableEndpoints[sync][endpoint['uuid']] = endpoint
                    tableEndpoints[sync][endpoint['uuid']]['state'] = state['state']
        if not clusterName == None:
            tableEndpoints['clusterName'] = clusterName
        log.warning(f"get_table_endpoints result {tableEndpoints}")
        return tableEndpoints
        """
    """TODO - Delete after testing
    def get_db_name(cluster, endpoint):
        database = server.clusters.endpoints.select('dbname', where={'uuid': endpoint, 'cluster': cluster})
        if len(database) > 0:
            return database[0]['dbname']
        log.error(f"No DB found with {cluster} endpoint {endpoint}")
    """
    @server.trace
    def get_endpoint_url(path, action, **kw):
        trace = kw['trace']
        cachePath = '/cache/'.join(path.split('/table/'))
        if 'commit' in kw or 'cancel' in kw:
            action = 'commit' if 'commit' in kw else 'cancel'
            return trace(f'{cachePath}/txn/{action}')
        if 'cache' in kw:
            return trace(f'{cachePath}/{action}/{kw["cache"]}')
        else:
            return trace(f'{path}/{action}')

    #TODO - Not sure if still used, delete if not used.
    """
    @server.route('/cluster/<cluster>/tableconf/<table>/<conf>/<action>', methods=['POST'])
    @server.is_authenticated('pyql')
    def post_update_table_conf(cluster, table, conf, action, data=None):
        pyql = server.env['PYQL_UUID']
        quorum, rc = cluster_quorum()
        if not 'inQuorum' in quorum['quorum'] or quorum['quorum']['inQuorum'] == False:
            return {
                "message": f"cluster pyql is not in quorum",
                "error": f"quorum was not available"}, 500
        data = request.get_json() if data == None else data
        if conf == 'sync' and action == 'status':
            for endpoint in data:
                update = {
                    'set': {
                        **data[endpoint]
                    },
                    'where': {
                        'name': endpoint
                    }
                }
                r, rc = post_request_tables(pyql, 'state', 'update', update)
        elif conf == 'pause':
            update = {
                'set': {
                    'isPaused': True if action == 'start' else False
                },
                'where': {
                    'cluster': cluster, 'name': table
                }
            }
            r, rc = post_request_tables(pyql, 'tables', 'update', update)
        else:
            log.warning(f"post_update_table_conf called but no actions to take {table} in {cluster} with {data}")
            pass
        return {"message": f"updated {table} in {cluster} with {data}"}, 200
    """
    @server.trace
    def get_table_info(cluster, table, endpoints, **kw):
        trace = kw['trace']
        """TODO - Delete after testing
        endpoints = server.clusters.endpoints.select(
            '*', 
            join={'state': {'endpoints.uuid': 'state.uuid', 'endpoints.cluster': 'state.cluster'}}, 
            where={'state.cluster': cluster, 'state.tableName': table}
            )
        for endpoint in endpoints:
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}
        ####
        """
        tb = server.clusters.tables.select(
            '*',
            where={'cluster': cluster, 'name': table}
            )[0]
        tb['endpoints'] = {}
        for sync in ['inSync', 'outOfSync']:
            for endpoint in endpoints[sync]:
                path = endpoints[sync][endpoint]['path']
                db = endpoints[sync][endpoint]['dbname']
                name = endpoints[sync][endpoint]['name']
                tb['endpoints'][name] = endpoints[sync][endpoint]
                tb['endpoints'][name]['path'] = f"http://{path}/db/{db}/table/{tb['name']}"
        trace(f"completed {tb}")
        return tb

        """TODO - Delete after testing
        tableEndpointState = server.clusters.state.select(
            '*',
            where={'cluster': cluster, 'tableName': table}
            )
        for state in tableEndpointState:
            endpoint = state['name'].split(table)[0]
            sync = 'inSync' if endpoint in endpoints['inSync'] else 'outOfSync'
            if not endpoint in endpoints[sync]:
                log.warning("mismatch in state / endpoints table, maybe caused by a reboot, removing stale state entries")
                cluster_endpoint_delete(cluster, state['uuid'])
                continue
            path = endpoints[sync][endpoint]['path']    
            db = endpoints[sync][endpoint]['dbname']
            tb['endpoints'][state['name']] = state
            tb['endpoints'][state['name']]['path'] = f"http://{path}/db/{db}/table/{tb['name']}"
        return tb
        """
    @server.trace
    def post_request_tables(cluster, table, action, requestData=None, **kw):
        """
            use details=True as arg to return tb['endpoints'] in response
        """
        trace = kw['trace']
        pyqlTxnExceptions = {'transactions', 'jobs', 'state', 'tables'}
        tableEndpoints = get_table_endpoints(cluster, table, caller='post_request_tables', trace=kw['trace'])
        pyql = server.env['PYQL_UUID']
        try:
            if requestData == None:
                requestData = request.get_json()
        except Exception as e:
            message = f"missing input for {cluster} {table} {action}"
            return {"message": trace.exception(message)}, 400
        failTrack = []
        tb = get_table_info(cluster, table, tableEndpoints, trace=kw['trace'])
        def process_request():
            endpointResponse = {}
            epRequests = {}
            changeLogs = {'txns': []}
            requestUuid = str(uuid.uuid1())
            transTime = time.time()
            def get_txn(endpointUuid):
                return {
                    'endpoint': endpointUuid,
                    'uuid': requestUuid,
                    'tableName': table,
                    'cluster': cluster,
                    'timestamp': transTime,
                    'txn': {action: requestData}
                }
            for endpoint in tableEndpoints['inSync']:
                db = tableEndpoints['inSync'][endpoint]['dbname']
                path = tableEndpoints['inSync'][endpoint]['path']
                #db = get_db_name(cluster, endpoint)
                token = tableEndpoints['inSync'][endpoint]['token']
                epRequests[endpoint] = {
                    'path': get_endpoint_url(path, action, cache=requestUuid, trace=trace),
                    'data': {'txn': requestData, 'time': transTime},
                    'timeout': 1.0,
                    'headers': get_auth_http_headers('remote', token=token)
                }
            asyncResults = asyncrequest.async_request(epRequests, 'POST')

            for endpoint in tableEndpoints['inSync']:
                if not asyncResults[endpoint]['status'] == 200:
                    failTrack.append(f'{endpoint}{table}')
                    # start job to Retry for endpoint, or mark endpoint bad after testing
                    trace.warning(f"unable to {action} from endpoint {endpoint} using {requestData}")
                    error, rc = asyncResults[endpoint]['content'], asyncResults[endpoint]['status']
                else:
                    endpointResponse[endpoint] = asyncResults[endpoint]['content']
                    response, rc = asyncResults[endpoint]['status'], asyncResults[endpoint]['status']
            
            # At least 1 success in endpoint db change, need to mark failed endpoints out of sync
            # and create a changelog for resync
            if len(failTrack) > 0 and len(failTrack) < len(tableEndpoints['inSync']):
                trace.warning(f"At least 1 successful response & at least 1 failure to update of inSync endpoints {failTrack}")
                for failedEndpoint in failTrack:
                    # Marking failedEndpoint inSync=False for table endpoint
                    stateSet = {
                        "set": {"inSync": False},
                        "where": {"name": failedEndpoint}
                    }
                    if cluster == pyql and table == 'state' and 'state' in failedEndpoint:
                        # this is a pyql <endpoint>state table that is outOfSync,
                        epStateRequests = {}
                        for endpoint in endpointResponse:
                            db = tableEndpoints['inSync'][endpoint]['dbname']
                            path = tableEndpoints['inSync'][endpoint]['path']
                            token = tableEndpoints['inSync'][endpoint]['token']
                            epStateRequests[endpoint] = {
                                'path': get_endpoint_url(path, action, trace=trace),
                                'data': stateSet,
                                'timeout': 2.0,
                                'headers': get_auth_http_headers('remote', token=token)
                            }
                        trace(f"marking {failedEndpoint} as inSync=False on alive pyql state endpoints")
                        epStateResults = asyncrequest.async_request(epStateRequests, 'POST')
                        trace(f"marking {failedEndpoint} as inSync=False on alive pyql state endpoints - results: {epStateResults}")
                    else:
                        stateSet = {
                            "set": {"inSync": False},
                            "where": {"name": failedEndpoint}
                        }
                        post_request_tables(pyql, 'state', 'update', stateSet, trace=trace)
                    # Creating txn log for future replay for table endpoint
                    if not tb['endpoints'][failedEndpoint]['state'] == 'new':
                        if cluster == pyql and table in pyqlTxnExceptions:
                            trace.warning(f"{failedEndpoint} is outOfSync for pyql table {table}")
                            continue
                        # Write data to a change log for resyncing
                        changeLogs['txns'].append(
                            get_txn(tb['endpoints'][failedEndpoint]['uuid'])
                        )
            # All endpoints failed request - 
            elif len(failTrack) == len(tableEndpoints['inSync']):
                error=f"All endpoints failed request {failTrack} using {requestData} thus will not update logs" 
                return {"message": trace.error(error), "results": asyncResults}, 400
            else:
                # No InSync failures
                pass
            # Update any previous out of sync table change-logs, if any
            for outOfSyncEndpoint in tableEndpoints['outOfSync']:
                tbEndpoint = f'{outOfSyncEndpoint}{table}'
                if not tbEndpoint in tb['endpoints'] or not 'state' in tb['endpoints'][tbEndpoint]:
                    trace(f"outOfSyncEndpoint {tbEndpoint} may be new, not triggering resync yet {tb['endpoints']}")
                    continue
                if not tb['endpoints'][tbEndpoint]['state'] == 'new':
                    # Prevent writing transaction logs for failed transaction log changes
                    if cluster == pyql and table in pyqlTxnExceptions:
                        continue 
                    trace(f"new outOfSyncEndpoint {tbEndpoint} need to write to db logs")
                    changeLogs['txns'].append(
                        get_txn(tb['endpoints'][tbEndpoint]['uuid'])
                    )
                else:
                    trace(f"post_request_tables  table is new {tb['endpoints'][tbEndpoint]['state']}")
            
            def write_change_logs(changeLogs):
                if len(changeLogs['txns']) > 0:
                    # Better solution - maintain transactions table for transactions, table sync and logic is already available
                    for txn in changeLogs['txns']:
                        post_request_tables(pyql,'transactions','insert', txn, trace=trace)
            if cluster == pyql and table in pyqlTxnExceptions:
                pass
            else:
                write_change_logs(changeLogs)
            # Commit cached commands  
            epCommitRequests = {}
            for endpoint in endpointResponse:
                #db = get_db_name(cluster, endpoint)
                db = tableEndpoints['inSync'][endpoint]['dbname']
                path = tableEndpoints['inSync'][endpoint]['path']
                token = tableEndpoints['inSync'][endpoint]['token']
                epCommitRequests[endpoint] = {
                    'path': get_endpoint_url(path, action, commit=True, trace=trace),
                    'data': endpointResponse[endpoint],
                    'timeout': 2.0,
                    'headers': get_auth_http_headers('remote', token=token, trace=trace)
                }
            
            asyncResults = asyncrequest.async_request(epCommitRequests, 'POST')
            trace.info(asyncResults)
            # if a commit fails - due to a timeout or other internal - need to mark endpoint OutOfSync
            success, fail = set(), set()
            for endpoint in asyncResults:
                if not asyncResults[endpoint]["status"] == 200:
                    fail.add(endpoint)
                else:
                    success.add(endpoint)
            if len(success) == 0:
                return {
                    "message": trace.error(f"failed to commit {requestData} to inSync {table} endpoints"), 
                    "details": asyncResults}, 400
            if len(fail) > 0:
                for endpoint in fail:
                    stateSet = {
                        "set": {"inSync": False},
                        "where": {"name": f"{endpoint}{table}"}
                    }
                    post_request_tables(pyql, 'state', 'update', stateSet, trace=trace)
                trace.warning(f"commit failure for endpoints {fail}")
                if cluster == pyql and table in pyqlTxnExceptions:
                    pass
                else:
                    write_change_logs(
                        {'txns': [get_txn(endpoint) for endpoint in fail]}
                    )
            #pyql state table changes must be commited before logs to prevent loop
            """TODO - Delete after testing - state txn logs not used. 
            if cluster == pyql and table == 'state':
                write_change_logs(changeLogs)
                for failedEndpoint in failTrack:
                    post_request_tables(pyql, 'state', 'update', {'set': {'inSync': False}, 'where': {'name': failedEndpoint}})
            """

            return {"message": asyncResults}, 200
        if tb['isPaused'] == False:
            return process_request()
        else:
            if cluster == pyql and table == 'tables' or table == 'state' and action == 'update':
                # tables val isPaused / state inSync are values and we need to allow tables updates through if updating
                return process_request()
            trace.error(f"Table {table} is paused, Waiting 2.5 seconds before retrying")
            time.sleep(2.5)
            #TODO - create a counter stat to track how often this occurs
            if tb['isPaused'] == False:
                return process_request()
            else:
                #TODO - create a counter stat to track how often this occurs
                error = "table is paused preventing changes, maybe an issue occured during sync cutover, try again later"
                return {"message": trace.error(error)}, 500

    """ TODO - Delete after testing
    def pyql_get_inquorum_insync_endpoints(quorum, tbEndpoints):
        inQuorumInSync = []
        for endpoint in tbEndpoints['inSync']:
            if endpoint in quorum['quorum']['nodes']['nodes']:
                inQuorumInSync.append(endpoint)
        return inQuorumInSync
    def pyql_table_select_endpoints(cluster, table, quorum, tableEndpoints):
        endPointList = pyql_get_inquorum_insync_endpoints(quorum, tableEndpoints)
        if len(endPointList) == 0:
            #"this condition can occur if the cluster IS IN Quorum, but the only inSync i.e 'source of truth' for table is offline / outOfQuorum / path has changed"
            if table == 'jobs':
                #TODO - write unittest for testing recovery in this condition
                # jobs table is crucial for self-healing but consistency is not
                # mark all pyql jobs endpoints outOfSync
                updateWhere = {'set': {'inSync': False}, 'where': {'tableName': 'jobs', 'cluster': cluster}}
                post_request_tables(cluster, 'state', 'update', updateWhere)
                # delete all non-cron jobs in local jobs tb
                for jType in ['jobs', 'syncjobs']:
                    deleteWhere = {'where': {'type': jType}}
                    server.clusters.jobs.delete(**deleteWhere)
                # set this nodes' jobs table inSync=true
                updateWhere = {'set': {'inSync': True}, 'where': {'uuid': nodeId, 'tableName': 'jobs'}}
                post_request_tables(cluster, 'state', 'update', updateWhere)
                newTableEndpoints = get_table_endpoints(cluster, table, caller='pyql_table_select_endpoints')
                endPointList = pyql_get_inquorum_insync_endpoints(quorum, newTableEndpoints)
        return endPointList
        """
    @server.trace
    def pyql_reset_jobs_table(**kw):
        trace = kw['trace']
        """
        this func should run if the cluster is in the following conditions:
        - Is IN Quorum - at least 2/3 nodes are active
        - The only inSync i.e 'source of truth' for table is offline / outOfQuorum / path has changed
        """
        trace.warning(f"pyql_reset_jobs_table starting")
        pyql = server.env['PYQL_UUID']
        updateWhere = {'set': {'inSync': False}, 'where': {'tableName': 'jobs', 'cluster': pyql}}
        post_request_tables(pyql, 'state', 'update', updateWhere, trace=trace)
        # delete all non-cron jobs in local jobs tb
        for jType in ['jobs', 'syncjobs']:
            deleteWhere = {'where': {'type': jType}}
            server.clusters.jobs.delete(**deleteWhere)
         # set this nodes' jobs table inSync=true
        updateWhere = {'set': {'inSync': True}, 'where': {'uuid': nodeId, 'tableName': 'jobs'}}
        post_request_tables(pyql, 'state', 'update', updateWhere, trace=trace)
        trace.warning(f"pyql_reset_jobs_table finished")

    @server.trace
    def table_select(cluster, table, data=None, method='GET', quorum=None, **kw):
        trace = kw['trace']
        pyql = server.env['PYQL_UUID']
        """
        if not 'clusterName' in request.__dict__:
            if cluster == server.env['PYQL_UUID']:
                request.clusterName = 'pyql'
        """
        if quorum == None:
            quorum, rc = cluster_quorum(trace=trace)
        if not 'quorum' in quorum or quorum['quorum']['inQuorum'] == False:
            return {
                "message": trace.error(f"cluster pyql node {os.environ['HOSTNAME']} is not in quorum {quorum}"),
                "quorum": quorum}, 500

        #tableEndpoints = get_table_endpoints(cluster, table, caller='table_select')
        if cluster == pyql:
            return endpoint_probe(cluster, table, '/select', data=data, quorum=quorum, method=method, trace=trace)
            #endPointList = pyql_table_select_endpoints(cluster, table, quorum, tableEndpoints)
        else:
            return endpoint_probe(cluster, table, '/select', data=data,  method=method, trace=trace)
            #endPointList = [endpoint for endpoint in tableEndpoints['inSync']]

        """TODO - delete after testing
        log.warning(f"table select endPointList {endPointList}")

        if not len(endPointList) > 0:
            return {
                "status": 500, "message": f"no inSync endpoints found in cluster {cluster}",
                "note": endPointList}, 500
        while len(endPointList) > 0:
            epIndex = randrange(len(endPointList))
            endpoint = endPointList[epIndex]
            db = tableEndpoints['inSync'][endpoint]['dbname']
            headers = get_auth_http_headers('remote', token=tableEndpoints['inSync'][endpoint]['token'])
            try:
                if method == 'GET':
                    if cluster == pyql and endpoint == nodeId:
                        return {'data': server.data['cluster'].tables[table].select('*')}, 200
                    r = requests.get(
                        get_endpoint_url(cluster, endpoint, db, table, 'select'),
                        headers=headers,
                        timeout=4.0
                        )
                    break
                else:
                    data = request.get_json() if data == None else data
                    if cluster == pyql and endpoint == nodeId:
                        return server.actions['select']('cluster', table, data)
                    r = requests.post(
                        get_endpoint_url(cluster, endpoint, db, table, 'select'),
                        headers=headers, 
                        data=json.dumps(data),
                        timeout=4.0
                        )
                    break
            except Exception as e:
                log.exception(f"Encountered exception accessing {endpoint} for {cluster} {table} select")
            if cluster == pyql: 
                quorum, rc = cluster_quorum(update=True)
                endPointList = pyql_table_select_endpoints(cluster, table, quorum, tableEndpoints)
            else:
                endPointList.pop(epIndex)
            continue
        try:
            return r.json(), r.status_code
        except Exception as e:
            log.exception("Exception encountered during table_select")
            return {"data": [], "error": repr(e)}, 400
        """
    server.cluster_table_select = table_select

    @server.trace
    def get_random_table_endpoint(cluster, table, quorum=None, **kw):
        trace = kw['trace']
        endpoints = get_table_endpoints(cluster, table, caller='get_random_table_endpoint', trace=trace)['inSync']
        inSyncEndpoints = [ep for ep in endpoints]
        if len(inSyncEndpoints) == 0 and table == 'jobs':
            pyql_reset_jobs_table()
            endpoints = get_table_endpoints(cluster, table, caller='get_random_table_endpoint', trace=trace)['inSync']
            inSyncEndpoints = [ep for ep in endpoints]
        while len(inSyncEndpoints) > 0:  
            if len(inSyncEndpoints) > 1:
                endpointChoice = inSyncEndpoints.pop(randrange(len(inSyncEndpoints)))
            else:
                endpointChoice = inSyncEndpoints.pop(0)
            if not quorum == None:
                if not endpointChoice in quorum['quorum']['nodes']['nodes']:
                    trace.warning(f"get_random_table_endpoint skipped pyql endpoint {endpointChoice} as not in quorum")
                    if len(inSyncEndpoints) == 0 and table == 'jobs':
                        pyql_reset_jobs_table(trace=trace)
                        endpoints = get_table_endpoints(cluster, table, caller='get_random_table_endpoint', trace=trace)['inSync']
                        inSyncEndpoints = [ep for ep in endpoints]
                    continue
            yield endpoints[endpointChoice]
        yield None
    @server.trace
    def endpoint_probe(cluster, table, path='', timeout=1.0, quorum=None, **kw):
        trace = kw['trace']
        data = None
        errors = []
        if request.method in ['POST', 'PUT']:
            try:
                data = request.get_json()
            except Exception as e:
                return {"error": trace.error("expected json input for request")}, 400
        for endpoint in get_random_table_endpoint(cluster, table, quorum):
            if endpoint == None:
                return {"message": trace(f"no inSync endpoints in cluster {cluster} table {table} or all failed - errors {errors}")}, 400
            try:
                url = f"http://{endpoint['path']}/db/{endpoint['dbname']}/table/{table}{path}"
                r, rc = probe(
                    url,
                    method=request.method if not 'method' in kw else kw['method'],
                    data=data if not 'data' in kw else kw['data'],
                    token=endpoint['token'],
                    timeout=timeout
                )
                if not rc == 200:
                    errors.append({endpoint['name']: trace.exception(f"non 200 rc encountered with {endpoint} {rc}")})
                    # Continue to try other endpoints
                    continue
                # Response OK returning values
                return r,rc
            except Exception as e:
                errors.append({endpoint['name']: trace.exception(f"exception encountered with {endpoint}")})
                continue
    
    @server.route('/cluster/<cluster>/table/<table>', methods=['GET', 'PUT', 'POST'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    def cluster_table(cluster, table, **kw):
        trace = kw['trace']
        if request.method == 'GET':
            return endpoint_probe(cluster, table, trace=trace)
        return cluster_table_insert(cluster, table, trace=trace)

    @server.route('/cluster/<cluster>/table/<table>/<key>', methods=['GET', 'POST', 'DELETE'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    def cluster_table_key(cluster, table, key, **kw):
        trace = kw['trace']
        if request.method == 'GET':
            return endpoint_probe(cluster, table, path=f'/{key}', trace=trace)
        data = None
        try:
            data = request.get_json()
        except Exception as e:
            return {"error": trace.error("expected json input for request")}, 400
        primary = server.clusters.tables.select(
            'config', 
            where={'cluster': cluster, 'name': table})
        primary = primary[0]['config'][table]['primaryKey']
        if request.method == 'POST':
            return table_update(cluster, table, {'set': data, 'where': {primary: key}}, trace=trace)
        if request.method == 'DELETE':
            return table_delete(cluster, table, {'where': {primary: key}}, trace=trace)

    @server.route('/cluster/<cluster>/table/<table>/config', methods=['GET'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    def cluster_table_config(cluster, table, **kw):
        trace = kw['trace']
        return endpoint_probe(cluster, table, path=f'/config', trace=trace)

    @server.route('/cluster/<cluster>/table/<table>/select', methods=['GET','POST'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    def cluster_table_select(cluster, table, **kw):
        trace = kw['trace']
        if request.method == 'GET':
            try:
                return table_select(cluster, table, trace=kw['trace'])
            except Exception as e:
                return {"error": trace.exception("error in cluster table select")}, 500
        else:
            return table_select(cluster, table, request.get_json(), 'POST', trace=kw['trace'])
    

    @server.route('/cluster/<cluster>/table/<table>/update', methods=['POST'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    def cluster_table_update(cluster, table, data=None, **kw):
        trace = kw['trace']
        data = request.get_json() if data == None else data
        return table_update(cluster, table, data, trace=trace)
    @server.trace
    def table_update(cluster, table, data=None, **kw):
        trace = kw['trace']
        return post_request_tables(
            cluster, table,'update', data, trace=trace)
    server.cluster_table_update = table_update
            
    @server.route('/cluster/<cluster>/table/<table>/insert', methods=['POST'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    def cluster_table_insert(cluster, table, **kw):
        trace = kw['trace']
        data = request.get_json()
        table_insert(cluster, table, data, trace=trace)
    @server.trace
    def table_insert(cluster, table, data=None, **kw):
        return post_request_tables(cluster, table, 'insert',  data, trace=trace)
    server.cluster_table_insert = table_insert

    @server.route('/cluster/<cluster>/table/<table>/delete', methods=['POST'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    @server.trace
    def cluster_table_delete(cluster, table, **kw):
        return post_request_tables(cluster, table, 'delete', request.get_json(), trace=trace)

    @server.route('/cluster/<cluster>/table/<table>/pause/<pause>', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_table_pause(cluster, table, pause, **kw):
        return table_pause(cluster, table, pause, trace=kw['trace'])
    @server.trace
    def table_pause(cluster, table, pause, **kw):
        trace=kw['trace']
        pyql = server.env['PYQL_UUID']
        pause = True if pause == 'start' else False
        pauseSet = {
            'set': {'isPaused': pause},
            'where': {'cluster': cluster, 'name': table}
        }
        result, rc = post_request_tables(pyql, 'tables', 'update', pauseSet, trace=kw['trace'])
        trace.warning(f'cluster_table_pause {cluster} {table} pause {pause} result: {result}')
        return result, rc

    @server.route('/cluster/<cluster>/table/<table>/state/<endpoint>', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_table_endpoint(cluster, table, endpoint, **kw):
        return table_endpoint_sync(cluster, table, endpoint, trace=kw['trace'])
    @server.trace
    def table_endpoint(cluster, table, endpoint, config=None, **kw):
        trace=kw['trace']
        """ Sets state 
        cluster: uuid, table: name, endpoint: uuid, 
        config: {'inSync': True|False, 'state': 'loaded|new'}
        lastModTime: float(time.time())
        
        """
        pyql = server.env['PYQL_UUID']
        setConfig = request.get_json() if config == None else config
        validInputs = ['inSync', 'state']
        for cfg in setConfig:
            if not cfg in validInputs:
                return {"error": trace.error(f"invalid input {cfg}, supported config inputs {validInputs}")}, 400
        syncSet = {
            'set': setConfig,
            'where': {'cluster': cluster, 'tableName': table, 'uuid': endpoint}
        }
        result, rc = post_request_tables(pyql, 'state', 'update', syncSet, trace=kw['trace'])
        trace.warning(f'cluster_table_endpoint_sync {cluster} {table} endpoint {endpoint} result: {result}')
        return result, rc
    
    """ TODO - Remove after testing

    @server.route('/cluster/<cluster>/table/<table>/state/<action>', methods=['POST'])
    @server.is_authenticated('pyql')
    def cluster_table_state(cluster, table, action): #TODO - This can probably be removed. 
        config = request.get_json()
        pyql = server.env['PYQL_UUID']
        if action == 'set':
            config = request.get_json()
            resp = {}
            for endpoint in config:
                resp[endpoint] = {}
                tableEndpoint = f'{endpoint}{table}'
                updateSet = {
                    'set': {'state': config[endpoint]['state']},
                    'where': {'name': tableEndpoint, 'cluster': cluster}
                    }
                resp[endpoint]['r'], resp[endpoint]['rc'] = post_request_tables(pyql, 'state', 'update', updateSet)
            return {"message": f"set {config} for {table}", "results": resp}, 200
        elif action == 'get':
            log.warning(f"##cluster_table_status /cluster/pyql/table/state/state/get input is {config}")
            tb = get_table_info(
                cluster, 
                table,
                get_table_endpoints(cluster, table, caller='cluster_table_state')
                )
            endpoints = {endpoint: tb['endpoints'][f'{endpoint}{table}'] for endpoint in config['endpoints']}
            return endpoints, 200
        else:
            return {"message": f"invalid action {action} provided"}, 400
    """
    
    """TODO - Delete after testing
    @server.route('/cluster/<cluster>/table/<table>/sync/<action>', methods=['GET','POST'])
    @server.is_authenticated('cluster')
    @cluster_name_to_uuid
    def cluster_table_sync(cluster, table, action):
        if action == 'status':
            if request.method == 'GET':
                endpoints = get_table_endpoints(cluster, table, caller='cluster_table_sync')
                tb = get_table_info(cluster, table, endpoints)
                return tb, 200
    """

    @server.route(f'/cluster/<cluster>/tablelogs/<table>/<endpoint>/<action>', methods=['GET','POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_get_table_endpoint_logs(cluster, table, endpoint, action, **kw):
        if request.method == 'GET':
            return get_table_endpoint_logs(cluster, table, endpoint, action, trace=kw['trace'])
        if request.method == 'POST' and action == 'commit':
            return commit_table_endpoint_logs(cluster, table, endpoint, trace=kw['trace'])
    @server.trace
    def get_table_endpoint_logs(cluster, table, endpoint, action, **kw):
        """
        requires cluster uuid for cluster
        """
        trace=kw['trace']
        pyql = server.env['PYQL_UUID']
        clusterTableEndpointTxns = {
            'select': None,
            'where': {
                'endpoint': endpoint,
                'cluster': cluster,
                'tableName': table
                }
        }
        if action == 'count':
            clusterTableEndpointTxns['select'] = ['uuid']

        if action == 'getAll':
            clusterTableEndpointTxns['select'] = ['*']
        response, rc = table_select(
            pyql, 
            'transactions',
            clusterTableEndpointTxns,
            'POST'
            )
        if not rc == 200:
            trace.error(f"get_cluster_table_endpoint_logs GET - non 200 rc encountered {response} {rc}")
            return response, rc
        if action == 'count':
            log.warning(f"# count completed")
            return {"availableTxns": len(response['data'])}, rc
        elif action == 'getAll':
            log.warning(f"# getAll completed")
            return response, rc
        else:
            return {"message": trace(f"get_cluster_table_endpoint_logs - invalid action provided")}, 400

    @server.trace
    def commit_table_endpoint_logs(cluster, table, endpoint, txns=None, **kw):
        """
            expects input 
            {'txns': ['uuid1', 'uuid2', 'uuid3']}
        """
        trace=kw['trace']
        pyql = server.env['PYQL_UUID']
        # get list of commited txns
        commitedTxns = request.get_json() if txns == None else txns
        for txn in commitedTxns['txns']:
            deleteTxn = {
                'where': {
                    'endpoint': endpoint,
                    'cluster': cluster,
                    'tableName': table,
                    'uuid': txn
                }
            }
            resp, rc = post_request_tables(pyql, 'transactions', 'delete', deleteTxn, trace=kw['trace'])
            if not rc == 200:
                trace.error(f"something abnormal happened when commiting txnlog {txn}")
        return {"message": trace(f"successfully commited txns")}, 200

    @server.route('/cluster/<clusterName>/join', methods=['GET','POST'])
    @server.trace
    @server.is_authenticated('cluster')
    def join_cluster(clusterName, **kw):
        trace=kw['trace']
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
            ]
        }
        if request.method=='GET':
            return required, 200
        else:
            trace.info(f"join cluster for {clusterName}")
            config = request.get_json()
            db = server.data['cluster']
            newEndpointOrDatabase = False
            jobsToRun = []
            bootstrap = False
            pyql = None
            
            #check if pyql is bootstrapped

            def check_bootstrap():
                clusters = server.clusters.clusters.select(
                    '*', where={'name': 'pyql'}
                )
                for cluster in clusters:
                    if cluster['name'] == 'pyql':
                        return True, cluster['id']
                return False, None
            bootstrap, pyql = check_bootstrap()

            if not bootstrap and clusterName == 'pyql':
                bootstrap_pyql_cluster(config)
                bootstrap = True
            
            clusters = server.clusters.clusters.select(
                '*', where={'name': 'pyql'}
            )
            

            clusters = server.clusters.clusters.select(
                '*', where={'owner': request.auth})
            if not clusterName in [cluster['name'] for cluster in clusters]:
                #Cluster does not exist, need to create
                # ('id', str, 'UNIQUE NOT NULL'),
                # ('name', str),
                # ('owner', str), # UUID of auth user who created cluster 
                # ('access', str), # {"alllow": ['uuid1', 'uuid2', 'uuid3']}
                # ('createdByEndpoint', str),
                # ('createDate', str)

                data = {
                    'id': str(uuid.uuid1()),
                    'name': clusterName,
                    'owner': request.auth, # added via @server.is_autenticated
                    'access': {'allow': [request.auth]},
                    'createdByEndpoint': config['name'],
                    'createDate': f'{datetime.now().date()}'
                    }
                post_request_tables(pyql, 'clusters', 'insert', data, trace=kw['trace'])
            clusterId = server.clusters.clusters.select(
                '*', where={
                        'owner': request.auth, 
                        'name': clusterName
                    })[0]['id']
            
            #check for existing endpoint in cluster: clusterId 
            endpoints = server.clusters.endpoints.select('uuid', where={'cluster': clusterId})
            if not config['database']['uuid'] in [endpoint['uuid'] for endpoint in endpoints]:
                #add endpoint
                newEndpointOrDatabase = True
                data = {
                    'uuid': config['database']['uuid'],
                    'dbname': config['database']['name'],
                    'path': config['path'],
                    'token': config['token'],
                    'cluster': clusterId
                }
                post_request_tables(pyql, 'endpoints', 'insert', data, trace=kw['trace'])

            else:
                #update endpoint latest path info - if different
                trace.warning(f"endpoint with id {config['database']['uuid']} already exists in {clusterName} {endpoints}")
                updateSet = {
                    'set': {'path': config['path']},
                    'where': {'uuid': config['database']['uuid']}
                }
                if len(endpoints) == 1 and clusterName == 'pyql':
                    #Single node pyql cluster - path changed
                    server.clusters.endpoints.update(
                        **updateSet['set'],
                        where=updateSet['where']
                    )
                else:
                    post_request_tables(pyql, 'endpoints', 'update', updateSet, trace=kw['trace'])
                    if clusterId == server.env['PYQL_UUID']:
                        post_request_tables(
                            pyql, 'state', 'update', 
                            {'set': {'inSync': False}, 
                            'where': {
                                'uuid': config['database']['uuid'],
                                'cluster': clusterId}}, trace=kw['trace'])
            tables = server.clusters.tables.select('name', where={'cluster': clusterId})
            tables = [table['name'] for table in tables]
            # if tables not exist, add
            newTables = []
            for table in config['tables']:
                for tableName, tableConfig in table.items():
                    if not tableName in tables:
                        newTables.append(tableName)
                        #JobIfy - create as job so config
                        data = {
                            'name': tableName,
                            'cluster': clusterId,
                            'config': tableConfig,
                            'isPaused': False
                        }
                        post_request_tables(pyql, 'tables', 'insert', data, trace=kw['trace'])
            # If new endpoint was added - update endpoints in each table 
            # so tables can be created in each endpoint for new / exsting tables
            if newEndpointOrDatabase == True:
                jobsToRun = [] # Resetting as all cluster tables need a job to sync on newEndpointOrDatabase
                tables = server.clusters.tables.select('name', where={'cluster': clusterId})
                tables = [table['name'] for table in tables]
                endpoints = server.clusters.endpoints.select('*', where={'cluster': clusterId})    
                state = server.clusters.state.select('name', where={'cluster': clusterId})
                state = [tbEp['name'] for tbEp in state]

                for table in tables:
                    for endpoint in endpoints:
                        tableEndpoint = f"{endpoint['uuid']}{table}"
                        if not tableEndpoint in state:
                            # check if this table was added along with endpoint, and does not need to be created 
                            loadState = 'loaded' if endpoint['uuid'] == config['database']['uuid'] else 'new'
                            if not table in newTables:
                                #Table arleady existed in cluster, but not in endpoint with same table was added
                                syncState = False
                                loadState = 'new'
                            else:
                                # New tables in a cluster are automatically marked in sync by endpoint which added
                                syncState = True
                            # Get DB Name to update
                            data = {
                                'name': tableEndpoint,
                                'state': loadState,
                                'inSync': syncState,
                                'tableName': table,
                                'cluster': clusterId,
                                'uuid': endpoint['uuid'], # used for syncing logs
                                'lastModTime': 0.0
                            }
                            post_request_tables(pyql, 'state', 'insert', data, trace=kw['trace'])
            else:
                if not bootstrap and clusterName == 'pyql':
                    trace.warning(f"{os.environ['HOSTNAME']} was not bootstrapped - create tablesync job for table state")
                    if clusterName == 'pyql':
                        cluster_tablesync_mgr('check', trace=kw['trace'])
                        return {"message": trace.info(f"re-join cluster {clusterName} for endpoint {config['name']} completed successfully")}, 200
            # Trigger quorum update using any new endpoints if cluster name == pyql
            if clusterName == 'pyql':
                cluster_quorum_check()
                # pyql setup - sets pyql_uuid in env 
                probe(
                    f"http://{config['path']}/pyql/setup",
                    'POST',
                    {'PYQL_UUID': clusterId},
                    token=config['token']
                )
                # auth setup - applys cluster service token in joining pyql node, and pulls key
                result, rc = probe(
                    f"http://{config['path']}/auth/setup/cluster",
                    'POST',
                    {
                        'PYQL_CLUSTER_SERVICE_TOKEN': server.env['PYQL_CLUSTER_SERVICE_TOKEN']
                    },
                    token=config['token']
                )
                trace.warning(f"completed auth setup for new pyql endpoint: result {result} {rc}")
            return {"message": trace.warning(f"join cluster {clusterName} for endpoint {config['name']} completed successfully")}, 200
    @server.trace
    def re_queue_job(job, **kw):
        job_update(job['type'], job['id'],'queued', {"message": "job was requeued"}, trace=kw['trace'])

    @server.route('/cluster/pyql/jobmgr/cleanup', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_jobmgr_cleanup(**kw):
        """
            invoked on-demand or by cron to check for stale jobs & requeue
        """ 
        trace=kw['trace']
        pyql = server.env['PYQL_UUID']
        jobs = table_select(pyql, 'jobs')[0]['data']
        for job in jobs:
            if not job['next_run_time'] == None:
                # Cron Jobs 
                if time.time() - float(job['next_run_time']) > 240.0:
                    if not job['node'] == None:
                        re_queue_job(job, trace=kw['trace'])
                        continue
                    else:
                        trace.error(f"job {job['id']} next_run_time is set but stuck for an un-known reason")
            if not job['start_time'] == None:
                timeRunning = time.time() - float(job['start_time'])
                if timeRunning > 240.0:
                    # job has been running for more than 4 minutes
                    trace.warning(f"job {job['id']} has been {job['status']} for more than {timeRunning} seconds - requeuing")
                    re_queue_job(job, trace=kw['trace'])
                if job['status'] == 'queued':
                    if timeRunning > 30.0:
                        trace.warning(f"job {job['id']} has been queued for more {timeRunning} seconds - requeuing")
                        re_queue_job(job, trace=kw['trace'])
            else:
                if job['status'] == 'queued':
                    # add start_time to check if job is stuck
                    post_request_tables(
                        pyql, 'jobs', 'update', 
                        {'set': {
                            'start_time': time.time()}, 
                        'where': {
                            'id': job['id']}}, trace=kw['trace'])
            if job['status'] == 'waiting':
                waitingOn = None
                for jb in jobs:
                    if 'nextJob' in jb['config']:
                        if jb['config']['nextJob'] == job['name']:
                            waitingOn = jb['id']
                            break
                if waitingOn == None:
                    trace.warning(f"Job {job['name']} was waiting on another job which did not correctly queue, queuing now.")
                    re_queue_job(job, trace=kw['trace'])
                    
        return {"message": trace.warning(f"job manager cleanup completed")}, 200

    @server.route('/cluster/jobqueue/<jobtype>', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_jobqueue(jobtype, **kw):
        """
            Used by jobworkers or tablesyncers to pull jobs from clusters job queues
            jobtype = 'job|syncjob|cron'
        """
        trace=kw['trace']
        queue = f'{jobtype}s' if not jobtype == 'cron' else jobtype

        pyql = server.env['PYQL_UUID']
        node = request.get_json()['node']
        quorumCheck, rc = cluster_quorum(trace=kw['trace'])
         # check this node is inQuorum and if worker requesting job is from an inQuorum node
        trace.warning(f"cluster_jobqueue - quorumCheck {quorumCheck}, {rc}")
        if not 'quorum' in quorumCheck or not quorumCheck['quorum']['inQuorum'] == True or not node in quorumCheck['quorum']['nodes']['nodes']:
            warning = f"{node} is not inQuorum with pyql cluster {quorumCheck}, cannot pull job"
            return {"message": trace.warning(warning)}, 200

        
        """
        jobEndpoints = get_table_endpoints(pyql, 'jobs', caller='cluster_jobqueue')
        if not len(jobEndpoints['inSync'].keys()) > 0:
            # trigger tablesync check - no inSync job endpoints available for
            cluster_tablesync_mgr('check')
        """
        while True:
            jobSelect = {
                'select': ['id', 'name', 'type', 'next_run_time', 'node'], 
                'where':{
                    'status': 'queued',
                    'type': queue
                }
            }
            if not jobtype == 'cron':
                jobSelect['where']['node'] = None
            trace("starting to pull list of jobs")
            jobList, rc = table_select(pyql, 'jobs', data=jobSelect, method='POST', quorum=quorumCheck, trace=kw['trace'])
            trace(f"finished pulling list of jobs - jobList {jobList} ")
            jobList = jobList['data']

            for i, job in enumerate(jobList):
                if not job['next_run_time'] == None:
                    #Removes queued job from list if next_run_time is still in future 
                    if not float(job['next_run_time']) < float(time.time()):
                        jobList.pop(i)
                    if not job['node'] == None:
                        if time.time() - float(job['next_run_time']) > 120.0:
                            trace(f"found stuck job assigned to node {job['node']} - begin re_queue job")
                            trace.error(f"job # {job['id']} may be stuck / inconsistent, updating to requeue")
                            re_queue_job(job, trace=kw['trace'])
                            trace(f"found stuck job assigned to node {job['node']} - finished re_queue job")
                            #jobUpdate = {'set': {'node': None}, 'where': {'id': job['id']}}
                            #post_request_tables(pyql, 'jobs', 'update', jobUpdate)

            if len(jobList) == 0:
                return {"message": trace("no jobs to process at this time")}, 200 
            if jobtype == 'cron':
                jobList = sorted(jobList, key=lambda job: job['next_run_time'])
                job = jobList[0]
            else:
                latest = 3 if len(jobList) >= 3 else len(jobList)
                jobIndex = randrange(latest-1) if latest -1 > 0 else 0
                job = jobList[jobIndex]
            
            jobSelect['where']['id'] = job['id']

            trace.warning(f"Attempt to reserve job {job} if no other node has taken ")

            jobUpdate = {'set': {'node': node}, 'where': {'id': job['id'], 'node': None}}
            result, rc = post_request_tables(pyql, 'jobs', 'update', jobUpdate, trace=kw['trace'])
            if not rc == 200:
                trace.error(f"failed to reserve job {job} for node {node}")
                continue

            # verify if job was reserved by node and pull config
            jobSelect['where']['node'] = node
            jobSelect['select'] = ['*']
            jobCheck, rc = table_select(pyql, 'jobs', data=jobSelect, method='POST', trace=kw['trace'])
            if len(jobCheck['data']) == 0:
                continue
            trace.warning(f"cluster_jobqueue - pulled job {jobCheck['data'][0]} for node {node}")
            return jobCheck['data'][0], 200

    @server.route('/cluster/job/<jobtype>/<uuid>/<status>', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_job_update(jobtype, uuid, status, **kw):
        return job_update(jobtype, uuid, status, trace=kw['trace'])

    def job_update(jobtype, uuid, status, jobInfo=None, **kw):
        pyql = server.env['PYQL_UUID']
        trace=kw['trace']
        try:
            jobInfo = request.get_json() if jobInfo == None else jobInfo
        except Exception as e:
            trace.exception("jobInfo is missing or nothing was provided")
        if jobInfo == None:
            jobInfo = {}
        if status == 'finished':
            updateFrom = {'where': {'id': uuid}}
            if jobtype == 'cron':
                cronSelect = {'select': ['id', 'config'], 'where': {'id': uuid}}
                job, rc = table_select(pyql, 'jobs', cronSelect, 'POST', trace=kw['trace'])[0]['data'][0]
                updateFrom['set'] = {
                    'node': None, 
                    'status': 'queued',
                    'start_time': None}
                if rc == 200:
                    updateFrom['set']['next_run_time'] = str(time.time()+ job['config']['interval'])
                else:
                    updateFrom['set']['next_run_time'] = str(time.time() + 25.0)
                    trace.error(f"error pulling cron job {uuid}, {job} {response} - proceeding to mark finished")
                return post_request_tables(pyql, 'jobs', 'update', updateFrom, trace=kw['trace']) 
            return post_request_tables(pyql, 'jobs', 'delete', updateFrom)
        if status == 'running' or status == 'queued':
            updateSet = {'lastError': {}, 'status': status}
            for k,v in jobInfo.items():
                if k =='start_time' or k == 'status':
                    updateSet[k] = v
                    continue
                updateSet['lastError'][k] = v
            updateWhere = {'set': updateSet, 'where': {'id': uuid}}
            if status =='queued':
                updateWhere['set']['node'] = None
                updateWhere['set']['start_time'] = None
            else:
                updateWhere['set']['start_time'] = str(time.time())
            return post_request_tables(pyql, 'jobs', 'update', updateWhere, trace=kw['trace'])

    @server.route('/cluster/<cluster>/table/<table>/recovery', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_table_sync_recovery(cluster, table, **kw):
        """
        expects cluster uuid input for cluster, table string
        """
        return table_sync_recovery(cluster, table, trace=kw['trace'])
    @server.trace
    def table_sync_recovery(cluster, table, **kw):
        """
            run when all table-endpoints are inSync=False
        """
        trace=kw['trace']
        #need to check quorum as all endpoints are currently inSync = False for table
        pyql = server.env['PYQL_UUID']
        if cluster == pyql:
            quorumCheck, rc = cluster_quorum(trace=kw['trace'])
            if not quorumCheck['quorum']['inQuorum'] == True:
                error = f"unable to perform table_sync_recovery while outOfQuorum - quorum {quorumCheck}"
                return {"error": trace.error(error)}, 500

        # Need to check all endpoints for the most up-to-date loaded table
        select = {'select': ['path', 'dbname', 'uuid'], 'where': {'cluster': cluster}}
        clusterEndpoints = server.clusters.endpoints.select(
            'path', 'dbname', 'uuid', 'token',
            where={'cluster': cluster}
        )
        latest = {'endpoint': None, 'lastModTime': 0.0}
        trace.warning(f"table_sync_recovery - cluster {cluster} endpoints {clusterEndpoints}")
        findLatest = {'select': ['lastModTime'], 'where': {'tableName': table}}
        for endpoint in clusterEndpoints:
            if cluster == pyql and not endpoint['uuid'] in quorumCheck['quorum']['nodes']['nodes']:
                trace.warning(f"table_sync_recovery - endpoint {endpoint} is not in quorum, so assumed as dead")
                continue
            dbname = endpoint['dbname'] if cluster == pyql else 'pyql'
            pyqlTbCheck, rc = probe(
                f"http://{endpoint['path']}/db/{dbname}/table/pyql/select",
                method='POST',
                token=endpoint['token'],
                data=findLatest,
                timeout=2.0,
                trace=kw['trace']
            )
            trace(f"table_sync_recovery - checking lastModTime on cluster {cluster} endpoint {endpoint}")
            if pyqlTbCheck['data'][0]['lastModTime'] > latest['lastModTime']:
                latest['endpoint'] = endpoint['uuid']
                latest['lastModTime'] = pyqlTbCheck['data'][0]['lastModTime']
        trace(f"table_sync_recovery latest endpoint is {latest['endpoint']}")
        updateSetInSync = {
            'set': {'inSync': True}, 
            'where': {
                'name': f"{latest['endpoint']}{table}"
                }
            }
        if cluster == pyql and table == 'state':
            #special case - cannot update inSync True via clusterSvcName - still no inSync endpoints
            for endpoint in clusterEndpoints:
                if not endpoint['uuid'] in quorumCheck['quorum']['nodes']['nodes']:
                    trace.warning(f"table_sync_recovery - endpoint {endpoint} is not in quorum, so assumed as dead")
                    continue
                stateUpdate, rc = probe(
                    f"http://{endpoint['path']}/db/cluster/table/state/update",
                    'POST',
                    updateSetInSync,
                    timeout=2.0,
                    trace=kw['trace']
                )
        else:
            post_request_tables(pyql, 'state', 'update', updateSetInSync, trace=kw['trace'])
            #cluster_table_update(pyql, 'state', updateSetInSync)
        trace.warning(f"table_sync_recovery completed selecting an endpoint as inSync -  {latest['endpoint']} - need to requeue job and resync remaining nodes")
        return {"message": trace("table_sync_recovery completed")}, 200

    @server.route('/cluster/pyql/tablesync/<action>', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_tablesync_mgr(action, **kw):
        return tablesync_mgr(action, trace=kw['trace'])
    @server.trace
    def tablesync_mgr(action, **kw):
        """
            invoked regularly by cron or ondemand to create jobs to sync OutOfSync Endpoints.
        """
        trace=kw['trace']
        pyql = server.env['PYQL_UUID']
        #quorumCheck, rc = cluster_quorum_update(trace=kw['trace'])
        if action == 'check':
            jobsToCreate = {}
            jobs = {}
            tables = server.clusters.tables.select('name', 'cluster')
            for table in tables:
                cluster = table['cluster']
                tableName = table['name']
                endpoints = get_table_endpoints(cluster,tableName, caller='tablesync_mgr',trace=kw['trace'])
                if not len(endpoints['inSync'].keys()) > 0:
                    trace.warning(f"cluster_tablesync_mgr - detected all endpoints for {cluster} {tableName} are outOfSync")
                    table_sync_recovery(cluster, tableName, trace=kw['trace'])
                    endpoints = get_table_endpoints(cluster,tableName, caller='tablesync_mgr', trace=kw['trace'])
                endpoints = endpoints['outOfSync']
                for endpoint in endpoints:
                    endpointPath = endpoints[endpoint]['path']
                    if not cluster in jobsToCreate:
                        jobsToCreate[cluster] = {}
                    if not tableName in jobsToCreate[table['cluster']]:
                        jobsToCreate[cluster][tableName] = []
                    jobsToCreate[cluster][tableName].append(endpointPath)
            for cluster in jobsToCreate:
                jobs[cluster] = []
                for table in jobsToCreate[cluster]:
                    # Add sync_table job for each table in cluster
                    jobs[cluster].append({
                        'job': f'sync_table_{cluster}_{table}',
                        'jobType': 'tablesync',
                        'cluster': cluster,
                        'table': table
                        })
            for cluster in jobs:
                if cluster == pyql:
                    order = ['state','tables','clusters', 'auth', 'endpoints', 'databases', 'jobs', 'transactions']
                    stateCheck = False
                    jobsToRunOrdered = []
                    while len(order) > 0:
                        lastPop = None
                        for job in jobs[cluster]:
                            if len(order) == 0:
                                break
                            if job['table'] == order[0]:
                                if order[0] == 'state':
                                    stateCheck = True
                                lastPop = order.pop(0)
                                jobsToRunOrdered.append(job)
                        if lastPop == None:
                            order.pop(0)
                    """TODO - Determine if this is needed later
                    if stateCheck:
                        for endpointPath in jobsToCreate['pyql']['state']:
                            #endpoints to mark ready
                            jobsToRunOrdered.append({
                                "job": f"markReadyJob{'-'.join(endpointPath.split('.'))}",
                                "jobType": "cluster",
                                "method": "POST",
                                "node": f"http://{endpointPath}",
                                "path": "/cluster/pyql/ready",
                                "data": {'ready': True}
                            }
                            )
                    """
                    wait_on_jobs(pyql, 0, jobsToRunOrdered)
                else:
                    for job in jobs[cluster]:
                        jobs_add('syncjobs', job, trace=kw['trace'])
            trace.info(f"cluster_tablesync_mgr created {jobs} for outofSync endpoints")
            return {"jobs": jobs}, 200
    @server.trace
    def table_copy(cluster, table, inSyncPath, inSyncToken, outOfSyncPath, outOfSyncToken, **kw):
        trace=kw['trace']
        tableCopy, rc = probe(f'{inSyncPath}/select', token=inSyncToken, trace=kw['trace'])
        if rc == 500:
            error = f"#CRITICAL - tablesyncer was not able to find an inSync endpoints"
            return trace.error(error), rc
        response, rc = probe(f'{outOfSyncPath}/sync', 'POST', tableCopy, token=outOfSyncToken, trace=kw['trace'])
        if rc == 400:
            if 'not found in database' in response['message']:
                # create table & retry resync
                tableConfig, rc = probe(f'{inSyncPath}', token=inSyncToken, trace=kw['trace'])
                response, rc = probe(f'{outOfSyncPath}/create', 'POST', tableConfig, token=outOfSyncToken, trace=kw['trace'])
                if not rc == 200:
                    failure = f"failed to create table using {tableConfig}"
                    return trace.error(failure), 500
                #Retry sync since new table creation
                response, rc = probe(f'{outOfSyncPath}/sync', 'POST', tableCopy, token=outOfSyncToken, trace=kw['trace'])

        trace.warning(f"#SYNC table_copy results {response} {rc}")
        trace.warning(f"#SYNC initial table copy of {table} in cluster {cluster} completed, need to sync changes now")
        return response, rc

    @server.route('/cluster/<cluster>/table/<table>/sync', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_table_sync_run(cluster, table, **kw):
        return table_sync_run(cluster, table, trace=kw['trace'])
    def table_sync_run(cluster, table, job=None, **kw):
        trace=kw['trace']
        syncResults = {}
        class tracker:
            def __init__(self):
                self.step = 0
            def incr(self):
                self.step+=1
            
        step = tracker()
        try:
            job = request.get_json() if job == None else job
        except Exception as e:
            return {"error": trace.error(f"missing or invalid job information provided")}, 400
        job = job['job']
        def track(message):
            trace.warning(f"tablesyncer {job} cluster {cluster} table {table} seq={step.step} {message}")
            step.incr()
            return message
            
        pyqlSyncExclusions = {'transactions', 'jobs', 'state', 'tables'}
        pyql = server.env['PYQL_UUID']
        # get table endpoints
        tableEndpoints = get_table_endpoints(cluster, table, caller='cluster_table_sync_run', trace=kw['trace'])
        track(f"table endpoints {tableEndpoints}")
        if len(tableEndpoints['inSync']) == 0:
            track(f"no inSync endpoints - running table_sync_recovery")
            table_sync_recovery(cluster, table, trace=kw['trace'])
        for endpoint in tableEndpoints['outOfSync']:
            # outOfSync endpoint to sync
            ep = tableEndpoints['outOfSync'][endpoint]

            uuid, path, token, db, tableState = ep['uuid'], ep['path'], ep['token'], ep['dbname'], ep['state']
            clusterId = ep['cluster']
            endpointPath = f"http://{path}/db/{db}/table/{table}"

            # inSync endpoint To sync against
            inSync = list(tableEndpoints['inSync'].keys())[random.randrange(len([k for k in tableEndpoints['inSync']]))]
            inSyncEndoint = tableEndpoints['inSync'][inSync] 
            inSyncPath =  f"http://{inSyncEndoint['path']}/db/{db}/table/{table}"
            inSyncToken = inSyncEndoint['token']

            # check if endpoint is alive
            r, rc = probe(f'http://{path}/pyql/node', trace=kw['trace'])
            if not rc == 200 and not rc==404:
                warning = f"endpoint {uuid} is not alive or reachable with path {path} - cannot issue sync right now"
                syncResults[endpoint] = track(warning)
                continue

            def load_table():
                track("load_table starting")
                if clusterId == pyql and table in pyqlSyncExclusions:
                    #need to blackout changes to these tables during copy as txn logs not generated
                    track(f"PYQL - issuing cutover")
                    r, rc = table_pause(cluster, table, 'start')
                    track(f"PYQL - cutover start result: {r} rc {rc}")
                    #message, rc = table_cutover(clusterId, table, 'start')
                    track(f"PYQL - starting table_copy")
                    r, rc = table_copy(clusterId, table, inSyncPath, inSyncToken, endpointPath, token, trace=kw['trace'])
                    track(f"PYQL - table_copy result: {r} rc: {rc}")
                    if rc == 500:
                        if 'not able to find an inSync endpoints' in r:
                            track("PYQL table_copy could not able to find an inSync endpoints, triggering table_sync_recovery")
                            r, rc = table_sync_recovery(clusterId, table, trace=kw['trace'])
                            track(f"PYQL table_sync_recovery result {r} rc {rc}")
                        else:
                            # Table create failed
                            track("PYQL - table create failed")
                    else:
                        track(f"PYQL - Marking table endpoint as inSync & loaded")
                        r, rc = table_endpoint(clusterId, table, uuid, {'inSync': True, 'state': 'loaded'}, trace=kw['trace'])
                        track(f'PYQL - marking table endpoint {uuid} - result: {r} rc: {rc}')
                        if table == 'state':
                            # as sync endpoint is pyql - state, need to manually set inSync True on itself
                            status, rc = probe(
                                f'{endpointPath}/update',
                                method='POST', 
                                data={'set': {
                                    'state': 'loaded', 'inSync': True},
                                    'where': {'uuid': endpoint, 'tableName': 'state'}
                                },
                                token=token,
                                trace=kw['trace']
                            )
                    r, rc = table_pause(clusterId, table, 'stop', trace=kw['trace'])
                    track(f'PYQL - end of cutover, resuming table result: {r} rc: {rc}')
                else: 
                    r, rc = table_copy(clusterId, table, inSyncPath, inSyncToken, endpointPath, token, trace=kw['trace'])
                    track(f"table_copy results: {r} {rc}")
                    if rc == 500:
                        if 'not able to find an inSync endpoints' in r:
                            track("table_copy could not able to find an inSync endpoints, triggering table_sync_recovery")
                            r, rc = table_sync_recovery(clusterId, table, trace=kw['trace'])
                            track(f"PYQL table_sync_recovery result {r} rc {rc}")
                        else:
                            # Table create failed
                            track("table create failed")
                track("load_table completed")
            #
            def sync_cluster_table_logs():
                tryCount = 0
                track('starting sync_cluster_table_logs')
                while True:
                    try:
                        logsToSync, rc = get_table_endpoint_logs(clusterId, table, uuid, 'getAll', trace=kw['trace'])
                        break
                    except Exception as e:
                        tryCount+=1
                        if tryCount <= 2:
                            track(f"Encountered exception trying to to pull tablelogs, retry # {tryCount}")
                            continue
                        error = track(f"error when pulling logs - {repr(e)}")
                        return {"error": trace.exception(error)}, 500
                commitedLogs = []
                track(f"logs to process - count {len(logsToSync)}")
                txns = sorted(logsToSync['data'], key=lambda txn: txn['timestamp'])
                for txn in txns:
                    transaction = txn['txn']
                    for action in transaction:
                        message, rc = probe(f'{endpointPath}/{action}', 'POST', transaction[action], token=token, trace=kw['trace'])
                        if rc == 200:
                            commitedLogs.append(txn['uuid'])
                        else:
                            track(f"#CRITICAL sync_cluster_table_logs - should not have happened, commiting logs for {uuid} {message} {rc}")
                # confirm txns are applied & remove from txns table
                #/cluster/<cluster>/tablelogs/<table>/<endpoint>/commit - POST
                if len(commitedLogs) > 0:
                    commitResult, rc = commit_table_endpoint_logs(cluster, table, uuid, {'txns': commitedLogs}, trace=kw['trace'])
                message = f"sync_cluster_table_logs completed for {cluster} {table}"
                track(message)
                return {"message": message}, 200
            
            # 
            if tableState == 'new':
                track("table never loaded, needs to be initialize")
                load_table()
            else:
                # Check for un-commited logs - otherwise full resync needs to occur.
                track("table already loaded, checking for change logs")
                count, rc = get_table_endpoint_logs(clusterId, table, uuid, 'count', trace=kw['trace'])
                if rc == 200:
                    if count['availableTxns'] == 0:
                        track("no change logs found for table, need to reload table - drop / load")
                        # Need to reload table - drop / load
                        load_table()
            track("starting to sync from change logs")
            sync_cluster_table_logs()

            if clusterId == pyql and table in pyqlSyncExclusions:
                pass
            else:
                track("completed initial pull of change logs & starting a cutover by pausing table")
                r, rc = table_pause(clusterId, table, 'start', trace=kw['trace'])
                #message, rc = table_cutover(clusterId, table, 'start')
                track(f"cutover result: {r} rc: {rc}")
                tableEndpoint = f'{endpoint}{table}'
                try:
                    track("starting post-cutover pull of change logs")
                    sync_cluster_table_logs()
                    track("finished post-cutover pull of change logs")
                except Exception as e:
                    trace.exception("sync_cluster_table_logs encountered an exception")
                    track("exception encountered during pull of change logs, aborting cutover")
                    r, rc = table_pause(clusterId, table, 'stop', trace=kw['trace'])
                    return {"error": track(f"exception encountered during pull of change logs")}
                track("setting TB endpoint as inSync=True, 'state': 'loaded'")
                r, rc = table_endpoint(clusterId, table, uuid, {'inSync': True, 'state': 'loaded'}, trace=kw['trace'])
                track(f"setting TB endpoint as inSync=True, 'state': 'loaded' result: {r} rc: {rc}")
                # Un-Pause
                track("completing cutover by un-pausing table")
                r, rc = table_pause(clusterId, table, 'stop', trace=kw['trace'])
                track(f"completing cutover result: {r} rc: {rc}")

            syncResults[endpoint] = track(f"finished syncing {uuid} for table {table} in cluster {clusterId}")
        message = track(f"finished syncing cluster {cluster} table {table}")
        return {"message": message, "results": syncResults[endpoint]}, 200

                    
    @server.route('/cluster/<jobtype>/add', methods=['POST'])
    @server.is_authenticated('pyql')
    @server.trace
    def cluster_jobs_add(jobtype, **kw):
        return jobs_add(jobtype, trace=kw['trace'])
        
    @server.trace
    def jobs_add(jobtype, job=None, **kw):
        """
        meant to be used by node workers which will load jobs into cluster job queue
        to avoiding delays from locking during change operations
        For Example:
        # Load a job into node job queue
        server.jobs.append({'job': 'job-name', ...})
        Or
        cluster_jobs_add('syncjobs', jobconfig, status='WAITING')

        """
        trace=kw['trace']
        pyql = server.env['PYQL_UUID']
        job = request.get_json() if job == None else job
        trace.warning(f"cluster {jobtype} add for job {job} started")
        jobId = f'{uuid.uuid1()}'
        jobInsert = {
            'id': jobId,
            'name': job['job'],
            'type': jobtype,
            'status': 'queued' if not 'status' in kw else kw['status'],
            'config': job
        }
        if jobtype == 'cron':
            jobInsert['next_run_time'] = str(float(time.time()) + job['interval'])
        else:
            jobCheck, rc = table_select(
                pyql, 'jobs', 
                {'select': ['id'], 'where': {'name': job['job']}},
                'POST',
                trace=kw['trace']
                )
            if rc == 200:
                jobCheck= jobCheck['data']
            else:
                return {"message": trace(f'job {job} not added, could not verify if job exists in table, try again later')}, 400
            if len(jobCheck) > 0:
                jobStatus = f"job {jobCheck[0]['id'] }with name {job['job']} already exists"
                trace.warning(jobStatus)
                return {
                    'message': jobStatus,
                    'jobId': job['job']
                }, 200

        response, rc = post_request_tables(pyql, 'jobs', 'insert', jobInsert, trace=kw['trace'])
        trace.warning(f"cluster {jobtype} add for job {job} finished - {response} {rc}")
        return {
            "message": f"job {job} added to jobs queue - {response}",
            "jobId": jobId}, rc
    

    server.internal_job_add(joinClusterJob)

    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        #Job to trigger cluster_quorum()
        initQuorum = {
            "job": "initQuorum",
            "jobType": "cluster",
            "method": "POST",
            "path": "/pyql/quorum",
            "data": None
        }
        initMarkReadyJob = {
            "job": "initReady",
            "jobType": "cluster",
            "method": "POST",
            "path": "/cluster/pyql/ready",
            "data": {'ready': True}
        }
        # Create Cron Jobs inside init node
        cronJobs = []
        cronJobs.append({
            'job': 'tablesync_check',
            'jobType': 'cron',
            "method": "POST",
            "path": "/cluster/pyql/tablesync/check",
            "interval": 30,
            "data": None
        })
        if 'PYQL_TYPE' in os.environ and os.environ['PYQL_TYPE'] == 'K8S':
            pass
        else:
            server.internal_job_add(initQuorum)
            server.internal_job_add(initMarkReadyJob)
            cronJobs.append({
                'job': 'clusterQuorum_check',
                'jobType': 'cron',
                "method": "POST",
                "path": "/pyql/quorum/check",
                "interval": 15,
                "data": None
            })
        cronJobs.append({
            'job': 'clusterJob_cleanup',
            'jobType': 'cron',
            'method': 'POST',
            'path': '/cluster/pyql/jobmgr/cleanup',
            'interval': 30,
            'data': None
        })
        for job in cronJobs:
            newCronJob = {
                "job": f"addCronJob{job['job']}",
                "jobType": 'cluster',
                "method": "POST",
                "path": "/cluster/cron/add",
                "data": job,
            }
            log.warning(f"adding job {job['job']} to internaljobs queue")
            server.internal_job_add(newCronJob)
        

    # Check for number of endpoints in pyql cluster, if == 1, mark ready=True
    quorum = server.clusters.quorum.select('*')
    # clear existing quorum
    for node in quorum:
        server.clusters.quorum.delete(where={'node': node['node']})

    if len(endpoints) == 1 or os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        readyAndQuorum = True
        health = 'healthy'
    else:
        server.clusters.state.update(inSync=False, where={'uuid': nodeId}) 
        readyAndQuorum = False
        health = 'unhealthy'
    # Sets ready false for any node with may be restarting as resync is required before marked ready

    server.clusters.quorum.insert(**{
        'node': nodeId,
        'nodes': {'nodes': [nodeId]},
        'inQuorum': readyAndQuorum,
        'health': health,
        'lastUpdateTime': float(time.time()),
        'ready': readyAndQuorum,
    })