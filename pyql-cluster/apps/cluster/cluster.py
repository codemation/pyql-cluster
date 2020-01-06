"""
App for handling pyql-endpoint cluster requests
#TODO - Disallow tablesync jobs to create against endpoints which are not reachable - i.e check first
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

    server.clusterJobs = {'jobs': [], 'syncjobs': []}
    server.quorum = {'status': True, 'nodes': []}
    server.cronJobs = {}

    #Ready check
    server.ready = False

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
    os.environ['HOSTNAME'] = '-'.join(os.environ['PYQL_NODE'].split('.'))

    # Table created only if 'init' is passed into os.environ['PYQL_CLUSTER_ACTION']
    tables = [
                {
                    "clusters": server.get_table_func('cluster', 'clusters')[0]
                },
                {
                    "endpoints": server.get_table_func('cluster', 'endpoints')[0]
                },
                {
                    "databases": server.get_table_func('cluster', 'databases')[0]
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
            ]
    joinClusterJob = {
        "job": f"{os.environ['HOSTNAME']}joinCluster",
        "jobType": "node" if os.environ['PYQL_CLUSTER_ACTION'] == 'init' else 'cluster',
        "method": "POST",
        "path": "/cluster/pyql/join",
        "data": {
            "name": os.environ['HOSTNAME'],
            "path": f"{os.environ['PYQL_NODE']}:{os.environ['PYQL_PORT']}",
            "database": {
                'name': "cluster",
                'uuid': dbuuid
            },
            "tables": tables if os.environ['PYQL_CLUSTER_ACTION'] == 'init' else []
        }
    }
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
            return {
                'name': config['name'],
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
                'database': config['database']['name'],
                'cluster': 'pyql',
                'config': cfg,
                'isPaused': False
            }
        def get_state_data(table):
            return {
                'name': f'{config["name"]}{table}',
                'state': 'loaded',
                'inSync': True,
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
        for table in ['clusters', 'endpoints', 'databases']:
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
        update_clusters()
    
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
            if server.ready == True:
                return {"ready": server.ready}, 200
            else:
                return {"ready": server.ready}, 400
        else:
            """
                expects:
                ready =  {'ready': True|False}
            """
            ready = request.get_json() if ready == None else ready
            server.ready = ready['ready']
            return {"ready": server.ready}, 200



    @server.route('/cluster/pyql/quorum/check', methods=['POST'])
    def cluster_quorum_check():
        return cluster_quorum(check=True)

    @server.route('/cluster/pyql/quorum', methods=['GET', 'POST'])
    def cluster_quorum(check=False):
        if request.method == 'POST' or check == True:
            if not 'pyql' in server.cluster:
                warning = f"{os.environ['HOSTNAME']} - pyql node is still syncing"
                log.warning(warning)
                return {"message": warning}, 200
            epRequests = {}
            tableEndpoints = server.cluster['pyql']['endpoints']
            epList = []
            for endpoint in tableEndpoints:
                epList.append(endpoint)
                endPointPath = tableEndpoints[endpoint]['path']
                endPointPath = f'http://{endPointPath}/cluster/pyql/quorum'
                epRequests[endpoint] = {'path': endPointPath, 'data': None}
            if len(epList) == 0:
                return {"message": f"pyql node {os.environ['HOSTNAME']} is still syncing"}, 200
            try:
                epResults = asyncrequest.async_request(epRequests, 'POST' if check == True else 'GET')
            except Exception as e:
                log.exception("Excepton found during cluster_quorum() check")
            inQuorum = []
            for endpoint in epResults:
                if epResults[endpoint]['status'] == 200:
                    inQuorum.append(endpoint)

            if float(len(inQuorum) / len(epList)) >= float(2/3):
                server.quorum['status'] = True
            else:
                server.quorum['status'] = False
            server.quorum['nodes'] = inQuorum
            return {"message": server.quorum},200
        else:
            return {'message':'OK', 'quorum': server.quorum['nodes'], 'inQuorum': server.quorum['status']}, 200

    @server.route('/cluster/<cluster>/endpoint/<endpoint>/table/<table>/updateState', methods=['POST'])
    def update_state(cluster, endpoint, table):
        db = server.data['cluster']
        tbEndpoint = f'{endpoint}{table}'
        state = db.tables['state'].select('*', where={'name': tbEndpoint})
        try:
            if len(state) == 1:
                server.cluster[cluster]['tables']['state'][tbEndpoint] = state[0] if type(state) == list else state
                path = server.cluster[cluster]['endpoints'][endpoint]['path']
                server.cluster[cluster]['tables']['state'][tbEndpoint]['path'] = path
        except Exception as e:
            log.exception(f"Encountered exception during update_state {state}")

    @server.route('/cluster/<cluster>/endpoint/<endpoint>/db/<database>/table/<table>/update')
    def update_tables(cluster, endpoint, database, table=None):
        """
            Updates the in-memory table state configuration
        """
        db = server.data['cluster']
        def update_tables_work(tableToWorkOn):
            if tableToWorkOn['name'] in server.cluster[cluster]['tables']:
                tb = server.cluster[cluster]['tables'][tableToWorkOn['name']]
                for tbKey, tbVal in tableToWorkOn.items():
                    # update values if they are different
                    tb[tbKey] = tableToWorkOn[tbKey] if not tableToWorkOn[tbKey] == tb[tbKey] else tb[tbKey]
            else:
                # Assign table as it did not exist yet.
                server.cluster[cluster]['tables'][tableToWorkOn['name']] = tableToWorkOn
            tb = server.cluster[cluster]['tables'][tableToWorkOn['name']]
            if not 'endpoints' in tb:
                tb['endpoints'] = {}
            update_state(cluster, endpoint, tableToWorkOn['name'])
            state = server.cluster[cluster]['tables']['state'][f'{endpoint}{tableToWorkOn["name"]}']
            dbName = database.split(f'{endpoint}_')[1]
            inSyncState = state['inSync']
            # prevent refresh of inSync value - occurs if endpoint was marked inSync = False and db update not yet completed 
            if state['name'] in tb['endpoints'] and 'ignoreInSync' in tb['endpoints'][state['name']]:
                if tb['endpoints'][state['name']]['ignoreInSync'] == True:
                    inSyncState = tb['endpoints'][state['name']]['inSync']
            tb['endpoints'][state['name']] = {
                'inSync': state['inSync'],
                'state': state['state'],
                'uuid':  state['uuid'],
                'lastModTime': state['lastModTime'],
                'path': f'http://{state["path"]}/db/{dbName}/table/{tb["name"]}'
            }

        if table is not None:
            tableSelect = db.tables['tables'].select(
            '*', 
            where={
                'cluster': cluster,
                'name': table
                }
            )[0]
            update_tables_work(tableSelect)
        else:
            tables = db.tables['tables'].select(
                '*', where={
                        'cluster': cluster
                        }
                    )
            # Update State Table if cluster is 'pyql'
            if cluster == 'pyql':
                for ind, table in enumerate(tables):
                    if table['name'] == 'state':
                        update_tables_work(table)
                        tables.pop(ind)
            for table in tables:
                update_tables_work(table)
                
    def update_databases(cluster, endpoint, tables=True):
        db = server.data['cluster']
        databases = db.tables['databases'].select('*', where={'cluster': cluster, 'endpoint': endpoint})
        for database in databases:
            server.cluster[cluster]['databases'][database['name']] = database
            if tables == True:
                update_tables(cluster, endpoint, database['name'])

    def update_endpoints(cluster, databases=True):
        db = server.data['cluster']
        endpoints = db.tables['endpoints'].select('*', where={'cluster': cluster})
        for endpoint in endpoints:
            server.cluster[cluster]['endpoints'][endpoint['name']] = endpoint
            if databases == True:
                update_databases(cluster, endpoint['name'])

    def update_cluster(cluster, endpoints=True):
        if server.cluster is not None:
            db = server.data['cluster']
            server.cluster[cluster] = db.tables['clusters'].select(
                '*',
                where={'name': cluster}
            )[0]
            server.cluster[cluster]['endpoints'] = dict()
            server.cluster[cluster]['databases'] = dict()
            server.cluster[cluster]['tables'] = dict()
            if endpoints == True:
                update_endpoints(cluster)

    def update_clusters():
        server.cluster= dict()
        db = server.data['cluster']
        clusters = db.tables['clusters'].select('name')
        for cluster in clusters:
            for _, name in cluster.items():
                update_cluster(name)
    def post_update_cluster_config(cluster, action, items, data=None):
        """
            invokes /cluster/<cluster>/config/<action>  on each cluster node
            which triggers update_clusters() on each node refreshing
            in-memory config
        """
        tableEndpoints = get_table_endpoints('pyql', 'clusters')
        data = request.get_json() if data == None else data
        epRequests = {}
        for endpoint in tableEndpoints['inSync']:
            endPointPath = tableEndpoints['inSync'][endpoint]['path']
            endPointPath = f'http://{endPointPath}/cluster/{cluster}/config/{action}/{items}'
            epRequests[endpoint]={'path': endPointPath, 'data': data}
        asyncrequest.async_request(epRequests, 'POST')      

    def get_endpoint_list(cluster):
        if cluster in server.cluster:
            endpointList = [endpoint for endpoint in server.cluster[cluster]['endpoints']]
            return endpointList
        else:
            return [os.environ['PYQL_CLUSTER_SVC']]
    @server.route('/cluster/<cluster>/table/<table>/path')
    def get_db_table_path(cluster, table):
        paths = {'inSync': {}, 'outOfSync': {}}
        tableEndpoints = get_table_endpoints(cluster, table)
        for pType in paths:
            for endpoint in tableEndpoints[pType]:
                dbName = get_db_name(cluster, endpoint)
                paths[pType][endpoint] = f"http://{server.cluster[cluster]['endpoints'][endpoint]['path']}/db/{dbName}/table/{table}"
        return paths, 200

    @server.route('/cluster/<cluster>/table/<table>/endpoints')
    def get_table_endpoints(cluster, table):
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}
        if cluster in server.cluster:
            for endpoint in server.cluster[cluster]['endpoints']:
                tableEndpoint = f'{endpoint}{table}'
                if not tableEndpoint in server.cluster[cluster]['tables'][table]['endpoints']:
                    log.warning(f"##get_table_endpoints - {tableEndpoint} not found in {server.cluster[cluster]['tables'][table]['endpoints']}")
                    #tableEndpoints['outOfSync'][tableEndpoint] = server.cluster[cluster]['tables']['state'][tableEndpoint]
                    if tableEndpoint in server.cluster[cluster]['tables']['state']:
                        server.cluster[cluster]['tables'][table]['endpoints'][tableEndpoint] = server.cluster['pyql']['tables']['state'][tableEndpoint]
                        server.cluster[cluster]['tables'][table]['endpoints'][tableEndpoint]
                    continue
                if server.cluster[cluster]['tables'][table]['endpoints'][tableEndpoint]['inSync'] == True:
                    tableEndpoints['inSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
                    tableEndpoints['inSync'][endpoint]['uuid'] = server.cluster['pyql']['tables']['state'][tableEndpoint]['uuid']
                else:
                    tableEndpoints['outOfSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
                    tableEndpoints['outOfSync'][endpoint]['uuid'] = server.cluster['pyql']['tables']['state'][tableEndpoint]['uuid']
        else:
            log.error("get_table_endpoints This should never happen")
            tableEndpoints['inSync']['temp'] =  {'path': os.environ['PYQL_CLUSTER_SVC']}
        log.info(f"get_table_endpoints finished for table {table}: status {tableEndpoints}")
        return tableEndpoints

    def get_db_name(cluster, endpoint):
        if cluster == 'pyql':
            return 'cluster'
        for database in server.cluster[cluster]['databases']:
            if endpoint == server.cluster[cluster]['databases'][database]['endpoint']:
                return database.split(f'{endpoint}_')[1]
        log.error(f"No DB found with {cluster} endpoint {endpoint}")

    def get_endpoint_url(cluster, endpoint, db, table, action, **kw):
        endPointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
        if 'commit' in kw:
            return f'http://{endPointPath}/db/{db}/cache/{table}/txn/commit'
        if 'cache' in kw:
            return f'http://{endPointPath}/db/{db}/cache/{table}/{action}/{kw["cache"]}'
        else:
            return f'http://{endPointPath}/db/{db}/table/{table}/{action}'

    @server.route('/cluster/<cluster>/tableconf/<table>/<conf>/<action>', methods=['POST'])
    def post_update_table_conf(cluster, table, conf, action, data=None):
        """
            current primary use is for updating sync status of a table & triggering db 
        """
        if server.quorum['status'] == False:
            return {
                "message": f"cluster pyql is not in quorum",
                "error": f"quorum was not available"}, 500
        setInSyncTrue = False
        data = request.get_json() if data == None else data
        jobsToRun = []
        if conf == 'sync' and action == 'status':
            for endpoint in data:
                tableEndpoint = f'{endpoint}{table}'
                update = {
                    'set': {
                        **data[endpoint]
                    },
                    'where': {
                        'name': tableEndpoint
                    }
                }
                if 'inSync' in data[endpoint] and data[endpoint]['inSync'] == False:
                    # Job will update pyql state in db - as we are first only updating memory.
                    job = {
                        'job': f'updateTableSyncStatus_{cluster}_{table}',
                        'jobType': 'cluster',
                        'method': 'POST',
                        'path': f'/cluster/pyql/table/state/update',
                        'data': update
                    }
                    #cluster_jobs_add('jobs', job)
                    server.jobs.append(job)
                else:
                    setInSyncTrue = True
        def update_pyql_nodes():
            if not cluster == 'pyql':
                tableEndpoints = get_table_endpoints('pyql', 'state')
            else:
                tableEndpoints = get_table_endpoints('pyql', table)
            log.warning(f'table endpoints for updating {table}/{conf}/{action} {tableEndpoints}')
            epRequests = {}
            for endpoint in tableEndpoints['inSync']:
                endPointPath = f"{tableEndpoints['inSync'][endpoint]['path']}/cluster/{cluster}/table/{table}/{conf}/{action}"
                epRequests[endpoint] = {'path': endPointPath, 'data': data}
            asyncResults = asyncrequest.async_request(epRequests, 'POST')
            log.warning(f"update_pyql_nodes result {asyncResults}")
            return asyncResults
        if setInSyncTrue == True:
            # Mark outOfSync endpoint InSync
            update_pyql_nodes()
            # Update DB's on all new InSync nodes
            cluster_table_update(
                'pyql', 'state', 
                {
                    'set': {'inSync': True, 'state': 'loaded'},
                    'where': {'name': tableEndpoint}
                })
        results = update_pyql_nodes()
            
        # create job to update tables
        for job in jobsToRun:
            server.jobs.append(job)
        return {"message": f"updated {table} in {cluster} with {data}"}, 200


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
        tb = server.cluster[cluster]['tables'][table]
        def process_request():
            endpointResponse = {}
            epRequests = {}
            changeLogs = {'txns': []}
            requestUuid = str(uuid.uuid1())
            transTime = time.time()
            for endpoint in tableEndpoints['inSync']:
                tb = server.cluster[cluster]['tables'][table]
                db = get_db_name(cluster, endpoint)
                epRequests[endpoint] = {
                    'path': get_endpoint_url(cluster, endpoint, db, table, action, cache=requestUuid),
                    'data': requestData
                }

            asyncResults = asyncrequest.async_request(epRequests, 'POST')
            for endpoint in tableEndpoints['inSync']:
                if not asyncResults[endpoint]['status'] == 200:
                    failTrack.append(endpoint)
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
                mesage, rc = post_update_table_conf(cluster, table,'sync','status', statusData)
                
                if not rc == 200:
                    quorumCheck, rc = cluster_quorum(check=True)
                    log.error(f"failed to update outOfSyncNode(s) {statusData} quorumCheck {quorumCheck}")
                    message = f"canceling request, - quorum {server.quorum['nodes']} inQuorum {server.quorum['status']}"
                    #TODO - need to rollback and cancel cached requests for inSync & not failTracked endpoints
                    log.error(message)
                    return {"message": message}, 500

                for failedEndpoint in failTrack:
                    tbEndpoint = f'{failedEndpoint}{table}'
                    if not tb['endpoints'][tbEndpoint]['state'] == 'new':
                        #endPointPath = server.cluster[cluster]['endpoints'][failedEndpoint]['path']
                        # Prevent writing transaction logs for failed transaction log changes
                        if cluster == 'pyql':
                            log.warning(f"{tbEndpoint} is outOfSync for pyql table {table}")
                            if table == 'transactions':
                                continue

                        # Write data to a change log for resyncing
                        changeLogs['txns'].append({
                                'endpoint': tb['endpoints'][tbEndpoint]['uuid'],
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
                    #TODO - Create recurring job to check for outOfSync endpoints & create job to sync
                    # Prevent writing transaction logs for failed transaction log changes
                    if cluster == 'pyql':
                        if table == 'transactions':
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
            if len(changeLogs['txns']) > 0:
                # Better solution - maintain transactions table for transactions, table sync and logic is already available
                for txn in changeLogs['txns']:
                    post_request_tables('pyql','transactions','insert', txn)

            # Commit cached commands  
            epCommitRequests = {}
            for endpoint in endpointResponse:
                db = get_db_name(cluster, endpoint)
                epCommitRequests[endpoint] = {
                    'path': get_endpoint_url(cluster, endpoint, db, table, action, commit=True),
                    'data': endpointResponse[endpoint]
                }
            asyncResults = asyncrequest.async_request(epCommitRequests, 'POST')
            return {"message": asyncResults}, 200
        if tb['isPaused'] == False:
            return process_request()
        else:
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
        if server.quorum['status'] == False:
            return {
                "message": f"cluster pyql node {os.environ['HOSTNAME']} is not in quorum",
                "error": f"quorum was not available"}, 500


        tableEndpoints = get_table_endpoints(cluster, table)
        endPointList = [endpoint for endpoint in tableEndpoints['inSync']]
        if not len(endPointList) > 0:
            return {"status": 500, "message": f"no endpoints found in cluster {cluster}"}
        data = request.get_json() if data == None else data

        while len(endPointList) > 0:
            epIndex = randrange(len(endPointList))
            endpoint = endPointList[epIndex]
            db = get_db_name(cluster, endpoint)
            try:
                if method == 'GET':
                    r = requests.get(
                        get_endpoint_url(cluster, endpoint, db, table, 'select'),
                            headers={'Accept': 'application/json'}
                            )
                    break
                else:
                    r = requests.post(
                        get_endpoint_url(cluster, endpoint, db, table, 'select'),
                        headers={'Accept': 'application/json', "Content-Type": "application/json"}, 
                        data=json.dumps(data)
                        )
                    break
            except Exception as e:
                log.error(f"Encountered exception accessing {endpoint} for {cluster} {table} select")
                #log.warning(f"marking endpoint {endpoint} outOfSync for {cluster} table {table}")
                #try:
                #    statusData = {endpoint: {'inSync': False}}
                #    mesage, rc = post_update_table_conf(cluster, table,'sync','status', statusData)
                #except Exception as e:
                #    log.error(f"Encountered exception marking table {table} outOfSync from table_select")
            #remove failed ep from endPointList & try others
            endPointList.pop(epIndex)
            continue
            
        try:
            return r.json(), r.status_code
        except Exception as e:
            log.exception("Exception encountered during table_select")
            #error = f"{r.text} {r.status_code}"
            log.error(f"{repr(e)}")
            return {"data": [], "error": f"{repr(e)}"}, 400


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
        tb = server.cluster[cluster]['tables'][table]
        if action == 'set':
            """
                Expected  input {endpoint: {'state': 'new|loaded'}}
            """
            for endpoint in config:
                tableEndpoint = f'{endpoint}{table}'
                tb['endpoints'][tableEndpoint]['state'] = config[endpoint]['state']
            return {"message": f"set {config} for {table}"}
        elif action == 'get':
            """
                Expected  input {'endpoints': ['endpoint1', 'endpoint2']}
            """
            log.info(f"##cluster_table_status /cluster/pyql/table/state/state/get input is {config}")
            endpoints = {endpoint: tb['endpoints'][f'{endpoint}{table}'] for endpoint in config['endpoints']}
            return endpoints, 200
        else:
            return {"message": f"invalid action {action} provided"}, 400
    
    @server.route('/cluster/<cluster>/table/<table>/sync/<action>', methods=['GET','POST'])
    def cluster_table_sync(cluster, table, action):
        config = request.get_json()
        if not cluster in server.cluster or not table in server.cluster[cluster]['tables']:
            update_cluster(cluster)
        tb = server.cluster[cluster]['tables'][table]
        if action == 'status':
            if request.method == 'GET':
                update_cluster(cluster)
                response = get_table_endpoints(cluster, table)
                return response, 200
            # Expected  input {endpoint: {'inSync': False}}
            for endpoint in config:
                tableEndpoint = f'{endpoint}{table}'
                if config[endpoint]['inSync'] == False:
                    # Create Job for resyncing table endpoint
                    #This Key prevents in-memory refreshes from database 
                    tb['endpoints'][tableEndpoint]['ignoreInSync'] = True
                else:
                    tb['endpoints'][tableEndpoint]['ignoreInSync'] = False
                tb['endpoints'][tableEndpoint]['inSync'] = config[endpoint]['inSync']
            log.info(f"###cluster_table_sync set {config} for {table}")
            return {"message": f"set {config} for {table}"}, 200
    @server.route('/cluster/<cluster>/table/<table>/pause/<action>', methods=['GET','POST'])
    def cluster_table_pause(cluster, table, action):
        if not cluster in server.cluster or not table in server.cluster[cluster]['tables']:
            update_cluster(cluster)
        tb = server.cluster[cluster]['tables'][table]
        if 'action' == 'start':
            tb['isPaused'] = True
        elif 'action' == 'stop':
            tb['isPaused'] = False
        elif 'action' == 'status':
            config = request.get_json()
            if request.method == 'GET':
                return {'isPaused': tb['isPaused']}, 200
            tb['isPaused'] = config['isPaused']
        else:
            pass
        return {'isPaused': tb['isPaused']}, 200

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

    @server.route('/clusters/<action>', methods=['POST'])
    def clusters_action(action):
        """
            /clusters/updateAll used to trigger /clusters/update on each cluster node.
        """
        if action == 'update':
            update_clusters()
            return {'message': f"clusters {' '.join(server.cluster.keys())} updated"}, 200
        elif action == 'updateAll':
            endpoints = get_table_endpoints('pyql', 'state')['inSync']
            results = []
            for endpoint in endpoints:
                r = requests.post(f'{endpoint["path"]}/clusters/update')
                results.append(r)
            message= f"clusters_action {action} completed"
            log.info(message)
            return {'message': message, 'results': [r.status_code for r in results]}, 200
        return {"message": f"invalid action {action} for /clusters/"}, 400


    @server.route('/cluster/<cluster>/config/<action>/<items>', methods=['POST'])
    def update_cluster_config(cluster, action, items):
        if action == 'update':
            if items == 'cluster':
                update_cluster(cluster)
                return server.cluster[cluster], 200
            elif items == 'endpoints':
                update_endpoints(cluster)
                return server.cluster[cluster], 200
            else:
                pass
        return {"message": f"invalid action {action} or item {item} provided"}, 400
        

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
            if not 'pyql' in server.cluster and clusterName == 'pyql':
                bootstrap_pyql_cluster(config)
                bootstrap = True

            if not clusterName in server.cluster:
                #Cluster does not exist, need to create
                #JobIfy - create as job so config is replicated
                data = {
                    'name': clusterName, 
                    'createdByEndpoint': config['name'],
                    'createDate': f'{datetime.now().date()}'
                    }
                post_request_tables('pyql', 'clusters', 'insert', data)
                update_cluster(clusterName, False)
            #check for existing endpoint in cluster: clusterName
            # If endpoint does not exists create
            if not config['name'] in server.cluster[clusterName]['endpoints']:
                newEndpointOrDatabase = True
                data = {
                    'name': config['name'],
                    'path': config['path'],
                    'cluster': clusterName
                }
                post_request_tables('pyql', 'endpoints', 'insert', data)
                update_endpoints(clusterName, True)
            # check for existing endpoint db's in cluster: clusterName
            # if db not exist, add
            endpointDatabase = f'{config["name"]}_{config["database"]["name"]}'
            if not endpointDatabase in server.cluster[clusterName]['databases']:
                newEndpointOrDatabase = True
                #JobIfy - create as job so config
                data = {
                    'name': endpointDatabase,
                    'cluster': clusterName,
                    'dbname': config['database']['name'],
                    'uuid': config['database']['uuid'],
                    'endpoint': config['name']
                }
                post_request_tables('pyql', 'databases', 'insert', data)
                update_databases(clusterName, config['name'], False)

            # if tables not exist, add
            newTables = []
            for table in config['tables']:
                for tableName, tableConfig in table.items():
                    if not tableName in server.cluster[clusterName]['tables']:
                        newTables.append(tableName)
                        #JobIfy - create as job so config
                        data = {
                            'name': tableName,
                            'database': config['database']['name'],
                            'cluster': clusterName,
                            'config': tableConfig,
                            'isPaused': False
                        }
                        post_request_tables('pyql', 'tables', 'insert', data)
                        update_tables(clusterName, config['name'], config['database'], tableName)
            # If new endpoint was added - update endpoints in each table 
            # so tables can be created in each endpoint for new / exsting tables
            if newEndpointOrDatabase == True:
                jobsToRun = [] # Resetting as all cluster tables need a job to sync on newEndpointOrDatabase 
                for table in server.cluster[clusterName]['tables']:
                    for endpoint in server.cluster[clusterName]['endpoints']:
                        tableEndpoint = f'{endpoint}{table}'
                        if not tableEndpoint in server.cluster[clusterName]['tables']['state']:
                            # check if this table was added along with endpoint, and does not need to be created 
                            loadState = 'loaded' if endpoint == config['name'] else 'new'
                            if not table in newTables:
                                #Table arleady existed in cluster, but not in endpoint with same table was added
                                syncState = False
                                loadState = 'new'
                            else:
                                # New tables in a cluster are automatically marked in sync
                                syncState = True
                            # Get DB Name to update
                            endpointDb = get_db_name(clusterName, endpoint)
                            endpointDb = server.cluster[clusterName]['databases'][f'{endpoint}_{endpointDb}']
                            data = {
                                'name': tableEndpoint,
                                'state': loadState,
                                'inSync': syncState,
                                'uuid': endpointDb['uuid'], # used for syncing logs 
                                'lastModTime': 0.0
                            }
                            post_request_tables('pyql', 'state', 'insert', data)
                            update_tables(clusterName, endpoint, endpointDb['name'], table)
            else:
                if not bootstrap:
                    log.warning(f"{os.environ['HOSTNAME']} was not bootstrapped - create tablesync job for table state")
                    if clusterName == 'pyql':
                        """
                        nodeJoinJobs = []
                        stateTableSync = {
                            'job': f'sync_table_pyql_state',
                            'jobType': 'tablesync',
                            'cluster': clusterName,
                            'table': table
                        }
                        nodeJoinJobs.append(stateTableSync)
 
                        log.warning(f"create job to run after job which marks /joined node as ready=True to receive be able to receive requests")
                        markReadyJob = {
                            "job": f"markReadyJob_{config['name']}",
                            "jobType": "cluster",
                            "method": "POST",
                            "node": config['path'],
                            "path": "/cluster/pyql/ready",
                            "data": {'ready': True}
                        }
                        nodeJoinJobs.append(markReadyJob)
                        """
                        # Trigger in-memory refresh from DB's on each cluster node.
                        post_update_cluster_config(clusterName, 'update', 'cluster')
                        cluster_tablesync_mgr('check')
                        #joinJobStart = wait_on_jobs(0, nodeJoinJobs)
                        return server.cluster[clusterName], 200
            # Trigger in-memory refresh from DB's on each cluster node.
            post_update_cluster_config(clusterName, 'update', 'cluster')

            return server.cluster[clusterName], 200

    def post_cluster_tables_config_sync(cluster, table=None):
        """
            checks for 'new' state endpoints in each cluster table and creates table in endpoint database
        """
        def table_config_sync(table):
            tb = server.cluster[cluster]['tables'][table]
            for database in server.cluster[cluster]['databases']:
                endpoint = server.cluster[cluster]['databases'][database]['endpoint']
                endpointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
                tableEndpoint = f'{endpoint}{table}'
                dbName = get_db_name(cluster, endpoint)
                if tb['endpoints'][tableEndpoint]['state'] == 'new':
                    r = requests.post(
                        f'http://{endpointPath}/db/{dbName}/table/create',
                        headers={'Accept': 'application/json', "Content-Type": "application/json"},
                        data=json.dumps(tb['config'])
                    )
                    is_requests_success(r, 'post_cluster_tables_config_sync')
        if table == None:
            for table in server.cluster[cluster]['tables']:
                table_config_sync(table)
        else:
            table_config_sync(table)
        return {"message": "post_cluster_tables_config_sync completed"}, 200
            

    @server.route('/cluster/<cluster>/sync', methods=['GET','POST'])
    def cluster_config_sync(cluster):
        # ALL DB's added to a cluster will attempt to mirror added tables to each other
        # Names of DB's will be unique within a cluster
        # DB's may exist in more than 1 cluster, but tables added in 1 cluster-db, should not exist in other clusters.
        # DB tables with same name as other DB's and different data, should be added to a different cluster.
        config = request.get_json()
        if 'table' in config:
            post_cluster_tables_config_sync(cluster, config['table'])
        else:
            post_cluster_tables_config_sync(cluster)
        return {"message": f"{cluster} config synced successfully"}, 200

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
        queue = f'{jobtype}s' if not jobtype == 'cron' else jobtype
        node = request.get_json()['node']
        node = '-'.join(node.split('.'))
        if not node in get_table_endpoints('pyql', 'jobs')['inSync']:
            warning = f"{node} is not inSync with pyql cluster yet, cannot pull job"
            log.warning(warning)
            return {"message": warning}, 200
        while True:
            jobSelect = {
                'select': ['id', 'next_run_time', 'node'], 
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

            log.info(f"Attempt to reserve job {job} if no other node has taken ")

            jobUpdate = {'set': {'node': node}, 'where': {'id': job['id'], 'node': None}}
            post_request_tables('pyql', 'jobs', 'update', jobUpdate)

            # verify if job was reserved by node and pull config
            jobSelect['where']['node'] = node
            jobSelect['select'] = ['*']
            jobCheck, rc = table_select('pyql', 'jobs', jobSelect, 'POST')
            if not len(jobCheck['data']) > 0:
                continue
            log.info(f"cluster_jobqueue - pulled job {jobCheck['data'][0]}")
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


    @server.route('/cluster/tablesync/<action>', methods=['POST'])
    def cluster_tablesync_mgr(action):
        """
            invoked regularly by cron or ondemand to create jobs to sync OutOfSync Endpoints.
        """
        quorumCheck, rc = cluster_quorum(check=True)
        if action == 'check':
            jobsToCreate = {}
            jobs = {}
            for cluster in server.cluster:
                for table in server.cluster[cluster]['tables']:
                    for endpoint in get_table_endpoints(cluster, table)['outOfSync']:
                        endpointPath = get_table_endpoints(cluster, table)['outOfSync'][endpoint]['path']
                        if not cluster in jobsToCreate:
                            jobsToCreate[cluster] = {}
                        if not table in jobsToCreate[cluster]:
                            jobsToCreate[cluster][table] = []
                        jobsToCreate[cluster][table].append(endpointPath)
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
                    order = ['clusters', 'endpoints', 'databases', 'tables', 'state', 'transactions', 'jobs']
                    stateCheck = False
                    jobsToRunOrdered = []
                    while len(order) > 0:
                        lastPop = None
                        for job in jobs[cluster]:
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
                        server.jobs.append(job)
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
    
    def add_cron_job_to_cluster(job):
        if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
            newCronJob = {
                "job": f"addCronJob{job['job']}",
                "jobType": 'cluster',
                "method": "POST",
                "path": "/cluster/cron/add",
                "data": job,
            }
            server.jobs.append(newCronJob)

    #Job to trigger cluster_quorum()
    initQuorum = {
        "job": "initQuorum",
        "jobType": "cluster",
        "method": "POST",
        "path": "/cluster/pyql/quorum",
        "data": None
    }

    update_clusters()
    server.jobs.append(joinClusterJob)
    server.jobs.append(initQuorum)
    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        initMarkReadyJob = {
            "job": "initReady",
            "jobType": "cluster",
            "method": "POST",
            "path": "/cluster/pyql/ready",
            "data": {'ready': True}
        }
        server.jobs.append(initMarkReadyJob)

    tableSyncCronJob = {
        'job': 'tablesync_check',
        'jobType': 'cron',
        "method": "POST",
        "path": "/cluster/tablesync/check",
        "interval": 60,
        "data": None
    }
    clusterQuorumCronJob = {
        'job': 'clusterQuorum_check',
        'jobType': 'cron',
        "method": "POST",
        "path": "/cluster/pyql/quorum/check",
        "interval": 15,
        "data": None
    }
    clusterJobCleanupCronJob = {
        'job': 'clusterJob_cleanup',
        'jobType': 'cron',
        'method': 'POST',
        'path': '/cluster/jobmgr/cleanup',
        'interval': 60,
        'data': None
    }
    for job in [clusterQuorumCronJob, tableSyncCronJob, clusterJobCleanupCronJob]:
        add_cron_job_to_cluster(job)

    

    

