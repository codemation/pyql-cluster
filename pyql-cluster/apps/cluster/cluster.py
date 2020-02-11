"""
App for handling pyql-endpoint cluster requests
#TODO - consider reducing stuck job detection time window or implement a call-back so can more quickly cleanup a stuck job
"""
def run(server):
    from flask import request
    import requests
    from datetime import datetime
    import time, uuid
    from random import randrange
    import json, os
    from apps.cluster import asyncrequest

    log = server.log

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

    os.environ['HOSTNAME'] = '-'.join(os.environ['PYQL_NODE'].split('.'))

    if not 'PYQL_CLUSTER_ACTION' in os.environ:
        os.environ['PYQL_CLUSTER_ACTION'] = 'join'

    # Table created only if 'init' is passed into os.environ['PYQL_CLUSTER_ACTION']
    tables = [
                {
                    "clusters": server.get_table_func('cluster', 'clusters')[0]
                },
                {
                    "endpoints": server.get_table_func('cluster', 'endpoints')[0]
                },
                {
                    "tables": server.get_table_func('cluster', 'tables')[0]
                },
                {
                    "state": server.get_table_func('cluster', 'state')[0]
                },
                {
                    "transactions": server.get_table_func('cluster', 'transactions')[0]
                },
                {
                    "jobs": server.get_table_func('cluster', 'jobs')[0]
                }
            ] if os.environ['PYQL_CLUSTER_ACTION'] == 'init' else []
    
    joinJobType = "node" if os.environ['PYQL_CLUSTER_ACTION'] == 'init' or len(endpoints) == 1 else 'cluster'

    joinClusterJob = {
        "job": f"{os.environ['HOSTNAME']}joinCluster",
        "jobType": joinJobType,
        "method": "POST",
        "path": "/cluster/pyql/join",
        "data": {
            "name": os.environ['HOSTNAME'],
            "path": f"{os.environ['PYQL_NODE']}:{os.environ['PYQL_PORT']}",
            "database": {
                'name': "cluster",
                'uuid': dbuuid
            },
            "tables": tables
        }
    }
    def probe(path, method='GET', data=None, timeout=1.0):
        url = f'{path}'
        try:
            if method == 'GET':
                r = requests.get(url, headers={'Accept': 'application/json'}, timeout=1.0)
            else:
                r = requests.post(url, headers={'Accept': 'application/json', "Content-Type": "application/json"}, data=json.dumps(data), timeout=timeout)
        except Exception as e:
            error = f"tablesyncer - Encountered exception when probing {path} - {repr(e)}"
            return error, 500
        try:
            return r.json(),r.status_code
        except:
            return r.text, r.status_code
    def wait_on_jobs(curInd, jobList, waitingOn=None):
        """
            job queing helper function - guarantees 1 job runs after the other by creating "waiting jobs" 
             dependent on the first job completing
        """
        if len(jobList) > curInd + 1:
            jobList[curInd]['nextJob'] = wait_on_jobs(curInd+1, jobList)
        if curInd == 0:
            return cluster_jobs_add(
                'syncjobs' if jobList[curInd]['jobType'] == 'tablesync' else 'jobs', 
                jobList[curInd])[0]['jobId']
        return cluster_jobs_add(
            'syncjobs' if jobList[curInd]['jobType'] == 'tablesync' else 'jobs', 
            jobList[curInd], status='waiting')[0]['jobId']
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
        
    def bootstrap_pyql_cluster(config):
        """
            runs if this node is targeted by /cluster/pyql/join and pyql cluster does not yet exist
        """
        log.info(f"bootstrap starting for {config['name']} config: {config}")
        def get_clusters_data():
            return {
                'name': 'pyql', 
                'createdByEndpoint': config['name'],
                'createDate': f'{datetime.now().date()}'
                }
        def get_endpoints_data():
            """
            return {
                'name': config['name'],
                'path': config['path'],
                'cluster': 'pyql'
            }
            """
            return {
                'uuid': config['database']['uuid'],
                'dbname': config['database']['name'],
                'path': config['path'],
                'cluster': 'pyql'
            }
        def get_databases_data():
            return {
                'name': f'{config["name"]}_{config["database"]["name"]}',
                'cluster': 'pyql',
                'uuid': config['database']['uuid'],
                'dbname': config['database']['name'],
                'endpoint': config['name']
            }
        def get_tables_data(table, cfg):
            return {
                'name': table,
                'cluster': 'pyql',
                'config': cfg,
                'isPaused': False
            }
        def get_state_data(table):
            return {
                'name': f'{config["database"]["uuid"]}{table}',
                'state': 'loaded',
                'inSync': True,
                'tableName': table,
                'cluster': 'pyql',
                'uuid': config['database']['uuid'], # used for syncing logs 
                'lastModTime': time.time()
            }
        def execute_request(endpoint, db, table, action, data):
            r = requests.post(
                        f'{endpoint}/db/{db}/table/{table}/{action}',
                        headers={'Accept': 'application/json', "Content-Type": "application/json"},
                        data=json.dumps(data),
                        timeout=0.5)
            is_requests_success(r,'bootstrap_pyql_cluster')
            return r
        data = {
            'clusters': get_clusters_data,
            'endpoints': get_endpoints_data,
            'databases': get_databases_data,
            'tables': get_tables_data,
            'state': get_state_data
        }
        localhost = f'http://localhost:{os.environ["PYQL_PORT"]}'
        for table in ['clusters', 'endpoints']:
            log.info(f"bootstrapping table {table}")
            execute_request(
                localhost, 
                'cluster', 
                table, 
                'insert',
                data[table]()
                )
        for table in config['tables']:
            for tableName, cfg in table.items():
                r = execute_request(
                    localhost,
                    'cluster',
                    'tables',
                    'insert',
                    get_tables_data(tableName, cfg)
                )
        for table in config['tables']:
            for name, cfg in table.items():
                r = execute_request(
                    localhost,
                    'cluster',
                    'state',
                    'insert',
                    data['state'](name)
                )
    
    @server.route('/pyql/node')
    def cluster_node():
        """
            returns node-id - to be used by workers instead of relying on pod ip:
        """
        log.warning(f"get nodeId called {nodeId}")
        return {"uuid": nodeId}, 200

    @server.route('/cluster/pyql/state/<action>', methods=['GET','POST'])
    def cluster_state(action):
        endpoints = get_table_endpoints('pyql', 'state')['inSync']
        if request.method == 'GET':
            pass
        if request.method == 'POST':
            if action == 'sync':
                pass

    @server.route('/cluster/pyql/ready', methods=['POST', 'GET'])
    def cluster_ready(ready=None):
        if request.method == 'GET':
            ready = server.clusters.quorum.select(
                'ready',
                where={'node': nodeId}
            )
            if ready[0]['ready'] == True:
                log.warning(f"cluster_ready check returned {ready[0]}")
                return ready[0], 200
            else:
                log.warning(f"cluster_ready check returned {ready[0]}")
                return ready[0], 400
        else:
            """
                expects:
                ready =  {'ready': True|False}
            """
            ready = request.get_json() if ready == None else ready
            updateSet = {
                'set': {'ready': ready['ready']}, 'where': {'node': nodeId}
            }
            server.clusters.quorum.update(**updateSet['set'], where=updateSet['where'])
            return ready, 200



    @server.route('/pyql/quorum/check', methods=['POST'])
    def cluster_quorum_check():
        pyqlEndpoints = server.clusters.endpoints.select('*', where={'cluster': 'pyql'})
        quorum = server.clusters.quorum.select('*')
        quorumNodes = {q['node']: q for q in quorum}
        if not len(pyqlEndpoints) > 0:
            warning = f"{os.environ['HOSTNAME']} - pyql node is still syncing"
            log.warning(warning)
            return {"message": warning}, 200
        epRequests = {}
        epList = []
        for endpoint in pyqlEndpoints:
            epList.append(endpoint['uuid'])
            endPointPath = endpoint['path']
            endPointPath = f'http://{endPointPath}/pyql/quorum'
            epRequests[endpoint['uuid']] = {'path': endPointPath, 'data': None}

        # check if quorum table contains stale endpoints & cleanup
        for endpoint in quorumNodes:
            if not endpoint in epList:
                server.clusters.quorum.delete(where={'node': endpoint})

        log.warning(f"quorum/check - running using {epRequests}")
        if len(epList) == 0:
            return {"message": f"pyql node {nodeId} is still syncing"}, 200
        try:
            epResults = asyncrequest.async_request(epRequests, 'POST')
        except Exception as e:
            log.exception("Excepton found during cluster_quorum() check")
        log.warning(f"quorum/check - results {epResults}")
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
        return {"message": f"quorum updated on {nodeId}", 'quorum': quorum},200

    @server.route('/pyql/quorum', methods=['GET', 'POST'])
    def cluster_quorum(check=False, get=False):
        if request.method == 'POST' or check == True:
            pyqlEndpoints = server.clusters.endpoints.select('*', where={'cluster': 'pyql'})
            if not len(pyqlEndpoints) > 0:
                warning = f"{os.environ['HOSTNAME']} - pyql node is still syncing"
                log.warning(warning)
                return {"message": warning}, 200
            epRequests = {}
            epList = []
            for endpoint in pyqlEndpoints:
                epList.append(endpoint['uuid'])
                endPointPath = endpoint['path']
                endPointPath = f'http://{endPointPath}/pyql/quorum'
                epRequests[endpoint['uuid']] = {'path': endPointPath}

            if len(epList) == 0:
                return {"message": f"pyql node {nodeId} is still syncing"}, 200
            try:
                epResults = asyncrequest.async_request(epRequests)
            except Exception as e:
                log.exception("Excepton found during cluster_quorum() check")
            inQuorum = []
            log.warning(f"epResults - {epResults}")
            for endpoint in epResults:
                if epResults[endpoint]['status'] == 200:
                    inQuorum.append(endpoint)
            isNodeInQuorum = None
            if float(len(inQuorum) / len(epList)) >= float(2/3):
                isNodeInQuorum = True
            else:
                isNodeInQuorum = False
            server.clusters.quorum.update(
                    **{'inQuorum': isNodeInQuorum, 
                    'nodes': {'nodes': inQuorum},
                    'lastUpdateTime': float(time.time())
                    }, 
                    where={'node': nodeId}
                    )
            quorum = server.clusters.quorum.select('*', where={'node': nodeId})[0]
            return {"message": f"quorum updated on {nodeId}", 'quorum': quorum},200
        else:
            try:
                quorum = server.clusters.quorum.select('*', where={'node': nodeId})[0]
            except Exception as e:
                log.error(f"exception occured during cluster_quorum for {nodeId} {quorum} ")
                log.error(f"{repr(e)}")
                quorum = []
            if quorum == None:
                log.error(f"exception occured during cluster_quorum for {nodeId} {quorum} ")
                quorum = []
            return {'message':'OK', 'quorum': quorum}, 200
   
    @server.route('/cluster/<cluster>/table/<table>/path')
    def get_db_table_path(cluster, table):
        paths = {'inSync': {}, 'outOfSync': {}}
        tableEndpoints = get_table_endpoints(cluster, table)
        tb = get_table_info(cluster, table, tableEndpoints)
        for pType in paths:
            for endpoint in tableEndpoints[pType]:
                dbName = get_db_name(cluster, endpoint)
                paths[pType][endpoint] = tb['endpoints'][f'{endpoint}{table}']['path']
        return paths, 200

    @server.route('/cluster/<cluster>/table/<table>/endpoints')
    def get_table_endpoints(cluster, table):
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}

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
        log.warning(f"get_table_endpoints result {tableEndpoints}")
        return tableEndpoints

    def get_db_name(cluster, endpoint):
        database = server.clusters.endpoints.select('dbname', where={'uuid': endpoint, 'cluster': cluster})
        if len(database) > 0:
            return database[0]['dbname']
        log.error(f"No DB found with {cluster} endpoint {endpoint}")

    def get_endpoint_url(cluster, endpoint, db, table, action, **kw):
        endPointPath = server.clusters.endpoints.select('path', where={'uuid': endpoint, 'cluster': cluster})
        endPointPath = endPointPath[0]['path']
        if 'commit' in kw or 'cancel' in kw:
            action = 'commit' if 'commit' in kw else 'cancel'
            return f'http://{endPointPath}/db/{db}/cache/{table}/txn/{action}'
        if 'cache' in kw:
            return f'http://{endPointPath}/db/{db}/cache/{table}/{action}/{kw["cache"]}'
        else:
            return f'http://{endPointPath}/db/{db}/table/{table}/{action}'

    @server.route('/cluster/<cluster>/tableconf/<table>/<conf>/<action>', methods=['POST'])
    def post_update_table_conf(cluster, table, conf, action, data=None):
        """
            current primary use is for updating sync status of a table & triggering db 
        """
        quorum, rc = cluster_quorum(False, True)

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
                r, rc = cluster_table_update('pyql', 'state', update)
        elif conf == 'pause':
            update = {
                'set': {
                    'isPaused': True if action == 'start' else False
                },
                'where': {
                    'cluster': cluster, 'name': table
                }
            }
            r, rc = cluster_table_update('pyql', 'tables', update)
        else:
            log.warning(f"post_update_table_conf called but no actions to take {table} in {cluster} with {data}")
            pass
        return {"message": f"updated {table} in {cluster} with {data}"}, 200
    def get_table_info(cluster, table, endpoints):
        tb = server.clusters.tables.select(
            '*',
            where={'cluster': cluster, 'name': table}
            )[0]
        tb['endpoints'] = {}
        tableEndpointState = server.clusters.state.select(
            '*',
            where={'cluster': cluster, 'tableName': table}
            )
        for state in tableEndpointState:
            endpoint = state['name'].split(table)[0]
            sync = 'inSync' if endpoint in endpoints['inSync'] else 'outOfSync'
            path = endpoints[sync][endpoint]['path']    
            db = endpoints[sync][endpoint]['dbname']
            tb['endpoints'][state['name']] = state
            tb['endpoints'][state['name']]['path'] = f"http://{path}/db/{db}/table/{tb['name']}"
        return tb

    def post_request_tables(cluster, table, action, requestData=None, **kw):
        """
            use details=True as arg to return tb['endpoints'] in response
        """
        tableEndpoints = get_table_endpoints(cluster, table)
        try:
            if requestData == None:
                requestData = request.get_json()
        except Exception as e:
            message = str(repr(e)) + f"missing input for {cluster} {table} {action}"
            log.exception(message)
            return {"message": message}, 400
        failTrack = []
        tb = get_table_info(cluster, table, tableEndpoints)
        def process_request():
            endpointResponse = {}
            epRequests = {}
            changeLogs = {'txns': []}
            requestUuid = str(uuid.uuid1())
            transTime = time.time()
            for endpoint in tableEndpoints['inSync']:
                db = get_db_name(cluster, endpoint)
                epRequests[endpoint] = {
                    'path': get_endpoint_url(cluster, endpoint, db, table, action, cache=requestUuid),
                    'data': requestData
                }

            asyncResults = asyncrequest.async_request(epRequests, 'POST')
            for endpoint in tableEndpoints['inSync']:
                if not asyncResults[endpoint]['status'] == 200:
                    failTrack.append(f'{endpoint}{table}')
                    # start job to Retry for endpoint, or mark endpoint bad after testing
                    log.warning(f"unable to {action} from endpoint {endpoint} using {requestData}")
                    error, rc = asyncResults[endpoint]['content'], asyncResults[endpoint]['status']
                else:
                    endpointResponse[endpoint] = asyncResults[endpoint]['content']
                    response, rc = asyncResults[endpoint]['status'], asyncResults[endpoint]['status']
            
            
            # At least 1 success in endpoint db change, need to mark failed endpoints out of sync
            # and create a changelog for resync
            if len(failTrack) > 0 and len(failTrack) < len(tableEndpoints['inSync']):
                log.warning(f"At least 1 successful response & at least 1 failure to update of inSync endpoints {failTrack}")

                # update state for outOfSyncNode
                statusData = {failedEndpoint: {'inSync': False} for failedEndpoint in failTrack}
                if cluster == 'pyql':
                    if not table == 'state':
                        mesage, rc = post_update_table_conf(cluster, table,'sync','status', statusData)
                else:
                    mesage, rc = post_update_table_conf(cluster, table,'sync','status', statusData)
                    log.warning(f"post_update_table_conf result{message} result {rc}")
                    if not rc == 200:
                        quorumCheck, rc = cluster_quorum(False, True)
                        log.error(f"failed to update outOfSyncNode(s) {statusData} quorumCheck {quorumCheck}")
                        log.error(f"rolling-back un-commited txn")
                        # RollBack cached commands  
                        epCancelRequests = {}
                        for endpoint in endpointResponse:
                            db = get_db_name(cluster, endpoint)
                            epCancelRequests[endpoint] = {
                                'path': get_endpoint_url(cluster, endpoint, db, table, action, cancel=True),
                                'data': endpointResponse[endpoint]
                            }
                        asyncResults = asyncrequest.async_request(epCancelRequests, 'POST')
                        log.error(message)
                        return {"message": message}, 500

                for failedEndpoint in failTrack:
                    if not tb['endpoints'][failedEndpoint]['state'] == 'new':
                        # Prevent writing transaction logs for failed transaction log changes
                        if cluster == 'pyql':
                            log.warning(f"{failedEndpoint} is outOfSync for pyql table {table}")
                            if table == 'transactions' or table == 'jobs' or table =='state':
                                continue

                        # Write data to a change log for resyncing
                        changeLogs['txns'].append({
                                'endpoint': tb['endpoints'][failedEndpoint]['uuid'],
                                'uuid': requestUuid,
                                'tableName': table,
                                'cluster': cluster,
                                'timestamp': transTime,
                                'txn': {action: requestData}
                            })
            # All endpoints failed request - 
            elif len(failTrack) == len(tableEndpoints['inSync']):
                log.error(f"All endpoints failed request {failTrack} using {requestData} thus will not update logs")
                return {"message": error}, rc if rc is not None else r.status_code
            else:
                # No InSync failures
                pass
            # Update any previous out of sync table change-logs, if any
            
            for outOfSyncEndpoint in tableEndpoints['outOfSync']:
                tbEndpoint = f'{outOfSyncEndpoint}{table}'
                if not tbEndpoint in tb['endpoints'] or not 'state' in tb['endpoints'][tbEndpoint]:
                    log.info(f"outOfSyncEndpoint {tbEndpoint} may be new, not triggering resync yet {tb['endpoints']}")
                    continue
                if not tb['endpoints'][tbEndpoint]['state'] == 'new':
                    # Prevent writing transaction logs for failed transaction log changes
                    if cluster == 'pyql':
                        if table == 'transactions' or table == 'jobs' or table == 'state':
                            continue
                    log.warning(f"new outOfSyncEndpoint {tbEndpoint} need to write to db logs")
                    changeLogs['txns'].append({
                            'endpoint': tb['endpoints'][tbEndpoint]['uuid'],
                            'uuid': requestUuid,
                            'tableName': table,
                            'cluster': cluster,
                            'timestamp': transTime,
                            'txn': {action: requestData}
                        })
                else:
                    log.info(f"post_request_tables  table is new {tb['endpoints'][tbEndpoint]['state']}")
            

            details = {'message': response, 'endpointStatus': tb['endpoints']} if 'details' in kw else response
            def write_change_logs():
                if len(changeLogs['txns']) > 0:
                    # Better solution - maintain transactions table for transactions, table sync and logic is already available
                    for txn in changeLogs['txns']:
                        post_request_tables('pyql','transactions','insert', txn)
            if cluster == 'pyql' and table == 'state':
                pass
            else:
                write_change_logs()
            # Commit cached commands  
            epCommitRequests = {}
            for endpoint in endpointResponse:
                db = get_db_name(cluster, endpoint)
                epCommitRequests[endpoint] = {
                    'path': get_endpoint_url(cluster, endpoint, db, table, action, commit=True),
                    'data': endpointResponse[endpoint]
                }
            asyncResults = asyncrequest.async_request(epCommitRequests, 'POST')
            #pyql state table changes must be commited before logs to prevent loop
            if cluster == 'pyql' and table == 'state':
                write_change_logs()
                for failedEndpoint in failTrack:
                    post_request_tables('pyql', 'state', 'update', {'set': {'inSync': False}, 'where': {'name': failedEndpoint}})

            return {"message": asyncResults}, 200
        if tb['isPaused'] == False:
            return process_request()
        else:
            if cluster == 'pyql' and table == 'tables' or table == 'state' and action == 'update':
                # tables val isPaused / state inSync are values and we need to allow tables updates through if updating
                return process_request()
            log.error(f"Table {table} is paused, Waiting 2.5 seconds before retrying")
            time.sleep(2.5)
            #TODO - create a counter stat to track how often this occurs
            if tb['isPaused'] == False:
                return process_request()
            else:
                #TODO - create a counter stat to track how often this occurs
                error = "table is paused preventing changes, maybe an issue occured during sync cutover, try again later"
                log.error(error)
                return {
                    "message": error
                    }, 500

    def table_select(cluster, table, data=None, method='GET'):
        quorum, rc = cluster_quorum(False, True)
        print(quorum)
        if quorum['quorum']['inQuorum'] == False:
            return {
                "message": f"cluster pyql node {os.environ['HOSTNAME']} is not in quorum",
                "error": f"quorum was not available"}, 500

        tableEndpoints = get_table_endpoints(cluster, table)

        # only attempts reads from endpoints in quorum
        endPointList = []
        for endpoint in tableEndpoints['inSync']:
            if cluster == 'pyql':
                if endpoint in quorum['quorum']['nodes']['nodes']:
                    endPointList.append(endpoint)
            else:
                endPointList.append(endpoint)
        if not len(endPointList) > 0:
            return {"status": 500, "message": f"no endpoints found in cluster {cluster}"}, 500
        data = request.get_json() if data == None else data

        while len(endPointList) > 0:
            epIndex = randrange(len(endPointList))
            endpoint = endPointList[epIndex]
            db = tableEndpoints['inSync'][endpoint]['dbname']
            try:
                if method == 'GET':
                    r = requests.get(
                        get_endpoint_url(cluster, endpoint, db, table, 'select'),
                        headers={'Accept': 'application/json'},
                        timeout=1.0
                        )
                    break
                else:
                    r = requests.post(
                        get_endpoint_url(cluster, endpoint, db, table, 'select'),
                        headers={'Accept': 'application/json', "Content-Type": "application/json"}, 
                        data=json.dumps(data),
                        timeout=1.0
                        )
                    break
            except Exception as e:
                log.error(f"Encountered exception accessing {endpoint} for {cluster} {table} select")
                log.error(repr(e))
                post_request_tables(
                    'pyql', 'state', 'update', 
                    {
                        'set': {'inSync': False}, 
                        'where': {'name': f'{endpoint}{table}'}
                    }
                )
            endPointList.pop(epIndex)
            continue
            
        try:
            return r.json(), r.status_code
        except Exception as e:
            log.exception("Exception encountered during table_select")
            log.error(f"{repr(e)}")
            return {"data": [], "error": f"{repr(e)}"}, 400

    @server.route('/cluster/<cluster>/table/<table>', methods=['GET'])
    def cluster_table_config(cluster, table):
        endpoints = get_table_endpoints(cluster, table)['inSync']
        inSyncEndpoints = [ep for ep in endpoints]
        if len(inSyncEndpoints) == 0:
            return {"message": f"no inSync endpoints to pull config in cluster {cluster} table {table}"}, 400
        if len(inSyncEndpoints) > 1:
            endpointChoice = inSyncEndpoints[randrange(len(inSyncEndpoints))]
        else:
            endpointChoice = inSyncEndpoints[0]
        endpoint = endpoints[endpointChoice]
        try:
            return probe(f"http://{endpoint['path']}/db/{endpoint['dbname']}/table/{table}")
        except Exception as e:
            error = f"exception encountered when pulling config from {endpoint}"
            log.error(error)
            log.exception(e)
            return {"error": error}, 500


    @server.route('/cluster/<cluster>/table/<table>/select', methods=['GET','POST'])
    def cluster_table_select(cluster, table):
        if request.method == 'GET':
            return table_select(cluster, table)
        else:
            return table_select(cluster, table, request.get_json(), 'POST')

    @server.route('/cluster/<cluster>/table/<table>/update', methods=['POST'])
    def cluster_table_update(cluster, table, data=None):
        return post_request_tables(
            cluster, table, 
            'update', 
            request.get_json() if data == None else data
        )
            
    @server.route('/cluster/<cluster>/table/<table>/insert', methods=['POST'])
    def cluster_table_insert(cluster, table):
        return post_request_tables(cluster, table, 'insert',  request.get_json())

    @server.route('/cluster/<cluster>/table/<table>/delete', methods=['POST'])
    def cluster_table_delete(cluster, table):
        return post_request_tables(cluster, table, 'delete', request.get_json())

    @server.route('/cluster/<cluster>/table/<table>/state/<action>', methods=['POST'])
    def cluster_table_state(cluster, table, action):
        config = request.get_json()
        if action == 'set':
            """
                Expected  input {endpoint: {'state': 'new|loaded'}}
            """
            config = request.get_json()
            for endpoint in config:
                tableEndpoint = f'{endpoint}{table}'
                server.clusters.state.update(
                    state=config[endpoint]['state'],
                    where={'name': tableEndpoint})
            return {"message": f"set {config} for {table}"}, 200
        elif action == 'get':
            """
                Expected  input {'endpoints': ['endpoint1', 'endpoint2']}
            """
            log.warning(f"##cluster_table_status /cluster/pyql/table/state/state/get input is {config}")
            tb = get_table_info(
                cluster, 
                table,
                get_table_endpoints(cluster, table)
                )
            endpoints = {endpoint: tb['endpoints'][f'{endpoint}{table}'] for endpoint in config['endpoints']}
            return endpoints, 200
        else:
            return {"message": f"invalid action {action} provided"}, 400
    
    @server.route('/cluster/<cluster>/table/<table>/sync/<action>', methods=['GET','POST'])
    def cluster_table_sync(cluster, table, action):
        if action == 'status':
            if request.method == 'GET':
                endpoints = get_table_endpoints(cluster, table)
                tb = get_table_info(cluster, table, endpoints)
                return tb, 200

    @server.route(f'/cluster/<cluster>/tablelogs/<table>/<endpoint>/<action>', methods=['GET','POST'])
    def get_cluster_table_endpoint_logs(cluster, table, endpoint, action, **kw):
        if request.method == 'GET' or 'GET' in kw:
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
                cluster, 
                'transactions',
                clusterTableEndpointTxns,
                'POST'
                )
            if not rc == 200:
                log.error(f"get_cluster_table_endpoint_logs GET - non 200 rc encountered {response} {rc}")
                return response, rc
            if action == 'count':
                return {"availableTxns": len(response['data'])}, rc
            elif action == 'getAll':
                log.info(f"#get_cluster_table_endpoint_logs getAll {response}")
                return response, rc
            else:
                return {"message": f"get_cluster_table_endpoint_logs -invalid action provided"}, 400
        if request.method == 'POST':
            if action == 'commit':
                """
                    expects input 
                    {'txns': ['uuid1', 'uuid2', 'uuid3']}
                """
                # get list of commited txns
                commitedTxns = request.get_json()
                for txn in commitedTxns['txns']:
                    deleteTxn = {
                        'where': {
                            'endpoint': endpoint,
                            'cluster': cluster,
                            'tableName': table,
                            'uuid': txn
                        }
                    }
                    resp, rc = post_request_tables('pyql', 'transactions', 'delete', deleteTxn)
                    if not rc == 200:
                        log.error(f"something abnormal happened when commiting txnlog {txn}")
                return {"message": f"successfully commited txns"}, 200

    @server.route('/cluster/<clusterName>/join', methods=['GET','POST'])
    def join_cluster(clusterName):
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
            log.info(f"join cluster for {clusterName}")
            config = request.get_json()
            db = server.data['cluster']
            newEndpointOrDatabase = False
            jobsToRun = []
            bootstrap = False
            
            #check if pyql is bootstrapped
            clusters = server.clusters.clusters.select('*')
            isBootstrapped = False
            for cluster in clusters:
                if cluster['name'] == 'pyql':
                    isBootstrapped = True

            if not isBootstrapped and clusterName == 'pyql':
                bootstrap_pyql_cluster(config)
                bootstrap = True

            clusters = server.clusters.clusters.select('*')
            if not clusterName in [cluster['name'] for cluster in clusters]:
                #Cluster does not exist, need to create
                #JobIfy - create as job so config is replicated
                data = {
                    'name': clusterName, 
                    'createdByEndpoint': config['name'],
                    'createDate': f'{datetime.now().date()}'
                    }
                post_request_tables('pyql', 'clusters', 'insert', data)

            #check for existing endpoint in cluster: clusterName 
            endpoints = server.clusters.endpoints.select('uuid', where={'cluster': clusterName})
            if not config['database']['uuid'] in [endpoint['uuid'] for endpoint in endpoints]:
                #add endpoint
                newEndpointOrDatabase = True
                data = {
                    'uuid': config['database']['uuid'],
                    'dbname': config['database']['name'],
                    'path': config['path'],
                    'cluster': clusterName
                }
                post_request_tables('pyql', 'endpoints', 'insert', data)

            else:
                #update endpoint latest path info - if different
                log.warning(f"endpoint with id {config['database']['uuid']} already exists in {clusterName} {endpoints}")
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
                    post_request_tables('pyql', 'endpoints', 'update', updateSet)
            tables = server.clusters.tables.select('name', where={'cluster': clusterName})
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
                            'cluster': clusterName,
                            'config': tableConfig,
                            'isPaused': False
                        }
                        post_request_tables('pyql', 'tables', 'insert', data)
            # If new endpoint was added - update endpoints in each table 
            # so tables can be created in each endpoint for new / exsting tables
            if newEndpointOrDatabase == True:
                jobsToRun = [] # Resetting as all cluster tables need a job to sync on newEndpointOrDatabase
                tables = server.clusters.tables.select('name', where={'cluster': clusterName})
                tables = [table['name'] for table in tables]
                endpoints = server.clusters.endpoints.select('*', where={'cluster': clusterName})    
                state = server.clusters.state.select('name', where={'cluster': clusterName})
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
                                'cluster': clusterName,
                                'uuid': endpoint['uuid'], # used for syncing logs
                                'lastModTime': 0.0
                            }
                            post_request_tables('pyql', 'state', 'insert', data)
            else:
                if not bootstrap and clusterName == 'pyql':
                    log.warning(f"{os.environ['HOSTNAME']} was not bootstrapped - create tablesync job for table state")
                    if clusterName == 'pyql':
                        cluster_tablesync_mgr('check')
                        return {"message": f"re-join cluster {clusterName} for endpoint {config['name']} completed successfully"}, 200
            # Trigger quorum update using any new endpoints if cluster name == pyql
            if clusterName == 'pyql':
                cluster_quorum_check()
            return {"message": f"join cluster {clusterName} for endpoint {config['name']} completed successfully"}, 200
    
    def re_queue_job(job):
        cluster_job_update(job['type'], job['id'],'queued')

    @server.route('/cluster/jobmgr/cleanup', methods=['POST'])
    def cluster_jobmgr_cleanup():
        """
            invoked on-demand or by cron to check for stale jobs & requeue
        """ 
        jobs = table_select('pyql', 'jobs')[0]['data']
        for job in jobs:
            if not job['next_run_time'] == None:
                # Cron Jobs 
                if time.time() - float(job['next_run_time']) > 240.0:
                    if not job['node'] == None:
                        re_queue_job(job)
                        continue
                    else:
                        log.error(f"job {job['id']} next_run_time is set but stuck for an un-known reason")
            if not job['start_time'] == None:
                if time.time() - float(job['start_time']) > 240.0:
                    # job has been running for more than 4 minutes
                    re_queue_job(job)
            if job['status'] == 'waiting':
                waitingOn = None
                for jb in jobs:
                    if 'nextJob' in jb['config']:
                        if jb['config']['nextJob'] == job['id']:
                            waitingOn = jb['id']
                            break
                if waitingOn == None:
                    log.warning(f"Job {job['id']} was waiting on another job which did not correctly queue, queuing now.")
                    re_queue_job(job)
                    
        return {"message": f"job manager cleanup completed"}, 200

    @server.route('/cluster/jobqueue/<jobtype>', methods=['POST'])
    def cluster_jobqueue(jobtype):
        """
            Used by jobworkers or tablesyncers to pull jobs from clusters job queues
            jobtype = 'job|syncjob|cron'
        """
        node = request.get_json()['node']
        quorumCheck, rc = cluster_quorum(False, True)
         # check this node is inQuorum and if worker requesting job is from an inQuorum node
        
        if not 'quorum' in quorumCheck or not quorumCheck['quorum']['inQuorum'] == True or not node in quorumCheck['quorum']['nodes']['nodes']:
            warning = f"{node} is not inQuorum with pyql cluster {quorumCheck}, cannot pull job"
            log.warning(warning)
            return {"message": warning}, 200

        queue = f'{jobtype}s' if not jobtype == 'cron' else jobtype

        jobEndpoints = get_table_endpoints('pyql', 'jobs')
        if not len(jobEndpoints['inSync'].keys()) > 0:
            # trigger tablesync check - no inSync job endpoints available for
            cluster_tablesync_mgr('check')
        while True:
            jobSelect = {
                'select': ['id', 'name', 'next_run_time', 'node'], 
                'where':{
                    'status': 'queued',
                    'type': queue
                }
            }
            if not jobtype == 'cron':
                jobSelect['where']['node'] = None

            jobList, rc = table_select('pyql', 'jobs', jobSelect, 'POST')
            jobList = jobList['data']

            for i, job in enumerate(jobList):
                if not job['next_run_time'] == None:
                    #Removes queued job from list if next_run_time is still in future 
                    if not float(job['next_run_time']) < float(time.time()):
                        jobList.pop(i)
                    if not job['node'] == None:
                        if time.time() - float(job['next_run_time']) > 120.0:
                            log.error(f"job # {job['id']} may be stuck / inconsistent, updating to requeue")
                            jobUpdate = {'set': {'node': None}, 'where': {'id': job['id']}}
                            post_request_tables('pyql', 'jobs', 'update', jobUpdate)

            if not len(jobList) > 0:
                info = f"queue {queue} - no jobs to process at this time"
                log.info(info)
                return {"message": info}, 200 

            latest = 3 if len(jobList) >= 3 else len(jobList)
            jobIndex = randrange(latest-1) if latest -1 > 0 else 0

            job = jobList[jobIndex]

            if jobtype == 'cron':
                lowest = time.time()
                # Run cron job waiting the longest to begin
                for i, job in enumerate(jobList):
                    if 'next_run_time' in job:
                        if float(job['next_run_time']) < lowest:
                            jobIndex = i

            jobSelect['where']['id'] = job['id']

            log.warning(f"Attempt to reserve job {job} if no other node has taken ")

            jobUpdate = {'set': {'node': node}, 'where': {'id': job['id'], 'node': None}}
            result, rc = post_request_tables('pyql', 'jobs', 'update', jobUpdate)
            if not rc == 200:
                log.error(f"failed to reserve job {job} for node {node}")
                continue

            # verify if job was reserved by node and pull config
            jobSelect['where']['node'] = node
            jobSelect['select'] = ['*']
            jobCheck, rc = table_select('pyql', 'jobs', jobSelect, 'POST')
            if not len(jobCheck['data']) > 0:
                continue
            log.warning(f"cluster_jobqueue - pulled job {jobCheck['data'][0]} for node {node}")
            return jobCheck['data'][0], 200

    @server.route('/cluster/<jobtype>/<uuid>/<status>', methods=['POST'])
    def cluster_job_update(jobtype, uuid, status, jobInfo={}):
        try:
            jobInfo = request.get_json() if jobInfo == None else jobInfo
        except:
            jobInfo = {}
        if status == 'finished':
            updateFrom = {'where': {'id': uuid}}
            if jobtype == 'cron':
                cronSelect = {'select': ['id', 'config'], 'where': {'id': uuid}}
                job = table_select('pyql', 'jobs', cronSelect, 'POST')[0]['data'][0]
                updateFrom['set'] = {
                    'node': None, 
                    'status': 'queued',
                    'start_time': None, 
                    'next_run_time': str(time.time()+ job['config']['interval'])}
                return post_request_tables('pyql', 'jobs', 'update', updateFrom) 
            return post_request_tables('pyql', 'jobs', 'delete', updateFrom)
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
            return post_request_tables('pyql', 'jobs', 'update', updateWhere)

    @server.route('/cluster/<cluster>/table/<table>/recovery', methods=['POST'])
    def table_sync_recovery(cluster, table):
        """
            run when all table-endpoints are inSync=False
        """
        #need to check quorum as all endpoints are currently inSync = False for table
        quorumCheck, rc = cluster_quorum(False, True)
        if quorumCheck['quorum']['inQuorum'] == True:
            # Need to check all endpoints for the most up-to-date loaded table
            select = {'select': ['path', 'dbname', 'uuid'], 'where': {'cluster': cluster}}
            clusterEndpoints = server.clusters.endpoints.select(
                'path', 'dbname', 'uuid', 
                where={'cluster': cluster}
            )
            latest = {'endpoint': None, 'lastModTime': 0.0}
            log.warning(f"table_sync_recovery - cluster {cluster} endpoints {clusterEndpoints}")
            findLatest = {'select': ['lastModTime'], 'where': {'tableName': table}}
            for endpoint in clusterEndpoints:
                if not endpoint['uuid'] in quorumCheck['quorum']['nodes']['nodes']:
                    log.warning(f"table_sync_recovery - endpoint {endpoint} is not in quorum, so assumed as dead")
                    continue
                pyqlTbCheck, rc = probe(
                    f"http://{endpoint['path']}/db/{endpoint['dbname']}/table/pyql/select",
                    'POST',
                    findLatest,
                    timeout=2.0
                )
                print(f"table_sync_recovery - checking lastModTime on cluster {cluster} endpoint {endpoint}")
                if pyqlTbCheck['data'][0]['lastModTime'] > latest['lastModTime']:
                    latest['endpoint'] = endpoint['uuid']
                    latest['lastModTime'] = pyqlTbCheck['data'][0]['lastModTime']
            print(f"table_sync_recovery latest endpoint is {latest['endpoint']}")
            updateSetInSync = {
                'set': {'inSync': True}, 
                'where': {
                    'name': f"{latest['endpoint']}{table}"
                    }
                }
            if cluster == 'pyql' and table == 'state':
                #special case - cannot update inSync True via clusterSvcName - still no inSync endpoints
                for endpoint in clusterEndpoints:
                    if not endpoint['uuid'] in quorumCheck['quorum']['nodes']['nodes']:
                        log.warning(f"table_sync_recovery - endpoint {endpoint} is not in quorum, so assumed as dead")
                        continue
                    stateUpdate, rc = probe(
                        f"http://{endpoint['path']}/db/cluster/table/state/update",
                        'POST',
                        updateSetInSync,
                        timeout=2.0
                    )
            else:
                cluster_table_update('pyql', 'state', updateSetInSync)
            print(f"table_sync_recovery completed selecting an endpoint as inSync -  {latest['endpoint']} - need to requeue job and resync remaining nodes")
            return {"message": "table_sync_recovery completed"}, 200

    @server.route('/cluster/tablesync/<action>', methods=['POST'])
    def cluster_tablesync_mgr(action):
        """
            invoked regularly by cron or ondemand to create jobs to sync OutOfSync Endpoints.
        """
        quorumCheck, rc = cluster_quorum(check=True)
        if action == 'check':
            jobsToCreate = {}
            jobs = {}
            tables = server.clusters.tables.select('name', 'cluster')
            for table in tables:
                cluster = table['cluster']
                tableName = table['name']
                endpoints = get_table_endpoints(cluster,tableName)
                if not len(endpoints['inSync'].keys()) > 0:
                    log.warning(f"cluster_tablesync_mgr - detected all endpoints for {cluster} {tableName} are outOfSync")
                    table_sync_recovery(cluster, tableName)
                    endpoints = get_table_endpoints(cluster,tableName)
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
                if cluster == 'pyql':
                    order = ['clusters', 'endpoints', 'databases', 'tables', 'state', 'jobs', 'transactions']
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
                    wait_on_jobs(0, jobsToRunOrdered)
                else:
                    for job in jobs[cluster]:
                        server.internal_job_add(job)
            log.info(f"cluster_tablesync_mgr created {jobs} for outofSync endpoints")
            return {"jobs": jobs}, 200
                    
    @server.route('/cluster/<jobtype>/add', methods=['POST'])
    def cluster_jobs_add(jobtype, job=None, **kw):
        """
            meant to be used by node workers which will load jobs into cluster job queue
            to avoiding delays from locking during change operations
            For Example:
            # Load a job into node job queue
            server.jobs.append({'job': 'job-name', ...})
        Or
            cluster_jobs_add('syncjobs', jobconfig, status='WAITING')

        """
        job = request.get_json() if job == None else job
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
                'pyql', 'jobs', 
                {'select': ['id'], 'where': {'name': job['job']}},
                'POST')
            if rc == 200:
                jobCheck= jobCheck['data']
            else:
                log.error(f"could not verify if job exists in table")
                return {"message": f"job {job} not added, could not verify existing name, try again later"}, 400
            if len(jobCheck) > 0:
                jobStatus = f"job {jobCheck[0]['id'] }with name {job['job']} already exists"
                log.warning(jobStatus)
                return {
                    'message': jobStatus,
                    'jobId': job['job']
                }, 200

        post_request_tables('pyql', 'jobs', 'insert', jobInsert)

        return {
            "message": f"job {job} added to jobs queue",
            "jobId": jobId}, 200
    

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
        server.internal_job_add(initQuorum)
        server.internal_job_add(initMarkReadyJob)
        # Create Cron Jobs inside init node 
        tableSyncCronJob = {
            'job': 'tablesync_check',
            'jobType': 'cron',
            "method": "POST",
            "path": "/cluster/tablesync/check",
            "interval": 30,
            "data": None
        }
        clusterQuorumCronJob = {
            'job': 'clusterQuorum_check',
            'jobType': 'cron',
            "method": "POST",
            "path": "/pyql/quorum/check",
            "interval": 15,
            "data": None
        }
        clusterJobCleanupCronJob = {
            'job': 'clusterJob_cleanup',
            'jobType': 'cron',
            'method': 'POST',
            'path': '/cluster/jobmgr/cleanup',
            'interval': 30,
            'data': None
        }
        for job in [clusterQuorumCronJob, tableSyncCronJob, clusterJobCleanupCronJob]:
            newCronJob = {
                "job": f"addCronJob{job['job']}",
                "jobType": 'cluster',
                "method": "POST",
                "path": "/cluster/cron/add",
                "data": job,
            }
            server.internal_job_add(newCronJob)
        

    # Check for number of endpoints in pyql cluster, if == 1, mark ready=True
    quorum = server.clusters.quorum.select('*')
    # clear existing quorum
    for node in quorum:
        server.clusters.quorum.delete(where={'node': node['node']})

    if len(endpoints) == 1 or os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        readyAndQuorum = True
    else:
        readyAndQuorum = False
    # Sets ready false for any node with may be restarting as resync is required before marked ready

    server.clusters.quorum.insert(**{
        'node': nodeId,
        'inQuorum': readyAndQuorum,
        'lastUpdateTime': float(time.time()),
        'ready': readyAndQuorum,
    })