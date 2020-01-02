"""
App for handling pyql-endpoint cluster requests

TODO: Create Job for handling the SYNC / RESYNC FLOW
Scenarios for SYNC / RESYNC
1. New DB endpoint was added to cluster, so existing tables are created and mirrored within DB and need to be synced.
- Table is created in new DB
- Job is started for syncing TB in new DB
- Worker claims sync  job & issues a startSync for TB, this starts log generation for new changes
- Worker completes initial insertions from select * of TB.
- Worker starts to pull changes from change logs 
- Worker completes pull of change logs & issues a cutover by pausing table.
- Worker checks for any new change logs that were commited just before the table was paused, and also syncs these.
- Worker sets new TB endpoint as inSync=True & unpauses TB
- SYNC job is completed
TODO: Endpoint state tracking
- Job schedule - every minute - update table state via a worker
- Job Event
        Name: Update table inSync Endpoints lastModTime & Update state tables for updated table on cluster nodes.
        Trigger:  Table Update - create job if a previously inSync node failed an update or a minute has passed since lastModTime
        Job Action:  update latest time.time() on current InSync table endpoints & trigger table state update on cluster nodes.


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
        "job": "joinCluster",
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
            return cluster_jobs_add('syncjobs', jobList[curInd])[0]['jobId']
        return cluster_jobs_add('syncjobs', jobList[curInd], status='waiting')[0]['jobId']
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
        
    @server.route('/cluster/pyql/quorum', methods=['GET', 'POST'])
    def cluster_quorum():
        if request.method == 'POST':
            epRequests = {}
            tableEndpoints = server.cluster['pyql']['endpoints']
            epList = []
            for endpoint in tableEndpoints:
                epList.append(endpoint)
                endPointPath = tableEndpoints[endpoint]['path']
                endPointPath = f'http://{endPointPath}/cluster/pyql/quorum'
                epRequests[endpoint] = {'path': endPointPath}
            try:
                epResults = asyncrequest.async_request(epRequests)
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
        state = db.tables['state'].select(
            '*',
            where={
                'name': tbEndpoint
                }
        )
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
        """
            r = requests.post(
                f'http://{endPointPath}/cluster/{cluster}/config/{action}/{items}',
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                data=data
            )
        """        

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
    @server.route('/cluster/<cluster>/table/<table>/endpoints/persistent')
    def get_persistent_table_endpoints(cluster, table):
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}
        #TODO - finish this for persistent checks of inSync status
        #TODO - maybe i do not actually need this
        pass

        
        

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
                        'job': 'updateTableSyncStatus',
                        'jobType': 'cluster',
                        'method': 'POST',
                        'path': f'/cluster/pyql/table/state/update',
                        'data': update
                    }
                    jobsToRun.append(job)
                else:
                    setInSyncTrue = True
        def update_pyql_nodes():
            if not cluster == 'pyql':
                tableEndpoints = get_table_endpoints('pyql', 'state')
            else:
                tableEndpoints = get_table_endpoints('pyql', table)
            log.info(f'table endpoints for updating {table}/{conf}/{action} {tableEndpoints}')
            epRequests = {}
            for endpoint in tableEndpoints['inSync']:
                endPointPath = f"{tableEndpoints['inSync'][endpoint]['path']}/cluster/{cluster}/table/{table}/{conf}/{action}"
                epRequests[endpoint] = {'path': endPointPath, 'data': data}
            asyncResults = asyncrequest.async_request(epRequests, 'POST')
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
                    message = f"canceling request, - quorum {server.quorum['nodes']} inQuorum {server.quorum['status']}"
                    #TODO - need to rollback and cancel cached requests for inSync & not failTracked endpoints
                    log.error(message)
                    return {"message": message}, 500

                for failedEndpoint in failTrack:
                    if not tb['endpoints'][failedEndpoint]['state'] == 'new':
                        endPointPath = server.cluster[cluster]['endpoints'][failedEndpoint]['path']
                        # Prevent writing transaction logs for failed transaction log changes
                        if cluster == 'pyql':
                            if table == 'transactions':
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
                        server.jobs.append({
                            'job': 'sync_table',
                            'jobType': 'tablesync',
                            'cluster': cluster,
                            'table': table
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
        tableEndpoints = get_table_endpoints(cluster, table)
        endPointList = [endpoint for endpoint in tableEndpoints['inSync']]
        if not len(endPointList) > 0:
            return {"status": 500, "message": f"no endpoints found in cluster {cluster}"}
        endpoint = endPointList[randrange(len(endPointList))]
        db = get_db_name(cluster, endpoint)
        data = request.get_json() if data == None else data
        if method == 'GET':
            r = requests.get(
                get_endpoint_url(cluster, endpoint, db, table, 'select'),
                    headers={'Accept': 'application/json'}
                    )
        else:
            r = requests.post(
                get_endpoint_url(cluster, endpoint, db, table, 'select'),
                headers={'Accept': 'application/json', "Content-Type": "application/json"}, 
                data=json.dumps(data)
                )
        try:
            return r.json(), r.status_code
        except Exception as e:
            log.exception("Exception encountered during table_select")
            error = f"{r.text} {r.status_code}"
            log.error(f"table_select error {error}")
            return {"data": [], "error": error}, r.status_code


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
        config = request.get_json()
        if not cluster in server.cluster or not table in server.cluster[cluster]['tables']:
            update_cluster(cluster)
        tb = server.cluster[cluster]['tables'][table]
        if 'action' == 'start':
            tb['isPaused'] = True
        elif 'action' == 'stop':
            tb['isPaused'] = False
        elif 'action' == 'status':
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

            #check if pyql is bootstrapped
            if not 'pyql' in server.cluster and clusterName == 'pyql':
                bootstrap_pyql_cluster(config)

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
                        jobsToRun.append({
                            'job': 'sync_table',
                            'jobType': 'tablesync',
                            'cluster': clusterName,
                            'table': tableName
                            })
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
                                # New table 
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
                    # Add sync_table job for each table in cluster
                    jobsToRun.append({
                        'job': 'sync_table',
                        'jobType': 'tablesync',
                        'cluster': clusterName,
                        'table': table
                        })
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

    @server.route('/cluster/jobmgr/<jobType>/<uuid>/<status>', methods=['POST'])
    def post_cluster_job_update_status(jobType, uuid, status, jobInfo=None):
        try:
            jobInfo = request.get_json() if jobInfo == None else jobInfo
        except:
            jobInfo = {}
        inSyncEndpoints = get_table_endpoints('pyql', 'jobs')['inSync']
        log.info(f"post_cluster_job_update_status inSyncEndpoints {inSyncEndpoints} for job {uuid}")
        epRequests = {}
        for endpoint in inSyncEndpoints:
            endpointPath = inSyncEndpoints[endpoint]['path']
            epRequests[endpoint] = {
                    'path': f'http://{endpointPath}/cluster/{jobType}/{uuid}/{status}',
                    'data': jobInfo
            }
        asyncResults = asyncrequest.async_request(epRequests, 'POST')
        log.info(f"post_cluster_job_update_status - results {asyncResults}")
        return {
            "message": f"updated {uuid} with status {status}",
            "results": asyncResults
            }, 200
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
    def cluster_job_update(jobtype, uuid, status):
        try:
            jobInfo = request.get_json()
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
        if action == 'check':
            jobsToCreate = {}
            jobs = {}
            for cluster in server.cluster:
                for table in server.cluster[cluster]['tables']:
                    for endpoint in get_table_endpoints(cluster, table)['outOfSync']:
                        if not cluster in jobsToCreate:
                            jobsToCreate[cluster] = {}
                        if not table in jobsToCreate[cluster]:
                            jobsToCreate[cluster][table] = []
                        jobsToCreate[cluster][table].append(endpoint)
            for cluster in jobsToCreate:
                jobs[cluster] = []
                for table in jobsToCreate[cluster]:
                    # Add sync_table job for each table in cluster
                    jobs[cluster].append({
                        'job': 'sync_table',
                        'jobType': 'tablesync',
                        'cluster': cluster,
                        'table': table
                        })
            for cluster in jobs:
                if cluster == 'pyql':
                    order = ['clusters', 'endpoints', 'databases', 'tables', 'state', 'transactions', 'jobs']
                    jobsToRunOrdered = []
                    while len(order) > 0:
                        lastPop = None
                        for job in jobs[cluster]:
                            if job['table'] == order[0]:
                                lastPop = order.pop(0)
                                jobsToRunOrdered.append(job)
                        if lastPop == None:
                            order.pop(0)
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
            'type': jobtype,
            'status': 'queued' if not 'status' in kw else kw['status'],
            'config': job
        }
        if jobtype == 'cron':
            jobInsert['next_run_time'] = str(float(time.time()) + job['interval'])
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

    update_clusters()
    server.jobs.append(joinClusterJob)

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
        "path": "/cluster/pyql/quorum",
        "interval": 15,
        "data": None
    }
    for job in [clusterQuorumCronJob, tableSyncCronJob]:
        add_cron_job_to_cluster(job)

    

    

