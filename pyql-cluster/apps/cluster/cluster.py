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
                    "jobs": server.get_table_func('cluster', 'transactions')[0]
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

    def bootstrap_pyql_cluster(config):
        """
            runs if this node is targeted by /cluster/pyql/join and pyql cluster does not yet exist
        """
        #print(type(config))
        print(f"bootstrap starting for {config['name']} config: {config}")
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
            #print(cfg)
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
            return requests.post(
                        f'{endpoint}/db/{db}/table/{table}/{action}',
                        headers={'Accept': 'application/json', "Content-Type": "application/json"},
                        data=json.dumps(data),
                        timeout=0.5)
        data = {
            'clusters': get_clusters_data,
            'endpoints': get_endpoints_data,
            'databases': get_databases_data,
            'tables': get_tables_data,
            'state': get_state_data
        }
        localhost = f'http://localhost:{os.environ["PYQL_PORT"]}'
        for table in ['clusters', 'endpoints', 'databases']:
            print(f"bootstrapping table {table}")
            execute_request(
                localhost, 
                'cluster', 
                table, 
                'insert',
                data[table]()
                )
        for table in config['tables']:
            for tableName, cfg in table.items():
                print(f"#######{tableName}")
                #print(type(cfg))
                #cfg = cfg[0] if type(cfg) == list else json.loads("{config: " + str(cfg) + "}")
                print(f"#######{tableName}")
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
                if not r.status_code == 200:
                    print(r.text)
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
                print(repr(e))
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
        print(f'##update_state state {state}')
        #assert type(state) == list, f"{state} not expected type list"
        #print(server.cluster[cluster]['tables'])
        server.cluster[cluster]['tables']['state'][tbEndpoint] = state[0] if type(state) == list else state
        path = server.cluster[cluster]['endpoints'][endpoint]['path']
        server.cluster[cluster]['tables']['state'][tbEndpoint]['path'] = path
        return server.cluster[cluster]['tables']['state'][tbEndpoint]

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
            print(f'###state for table {endpoint}{tableToWorkOn["name"]} {state}')
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
            print(f'###tables update_tables')
            # Update State Table if cluster is 'pyql'
            if cluster == 'pyql':
                for ind, table in enumerate(tables):
                    if table['name'] == 'state': #TODO - refactor to remove duplicate code
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
            #print(endpoint)
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
            #print(server.cluster[cluster])
            if endpoints == True:
                update_endpoints(cluster)

    def update_clusters():
        print(f"### update_clusters ")
        server.cluster= dict()
        db = server.data['cluster']
        clusters = db.tables['clusters'].select('name')
        #print(clusters)
        for cluster in clusters:
            for _, name in cluster.items():
                update_cluster(name)
        #print(server.cluster)
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
            #print(endpoint)
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
    @server.route('/cluster/<cluster>/table/<table>/endpoints')
    def get_table_endpoints(cluster, table):
        print(f"get_table_endpoints for {cluster} {table}")
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}
        if cluster in server.cluster:
            for endpoint in server.cluster[cluster]['endpoints']:
                #print(f"##get_table_endpoints - {server.cluster[cluster]['tables']['state']}")
                tableEndpoint = f'{endpoint}{table}'
                if not tableEndpoint in server.cluster[cluster]['tables'][table]['endpoints']:
                    print(f"##get_table_endpoints - {server.cluster[cluster]['tables'][table]['endpoints']}")
                    #tableEndpoints['outOfSync'][tableEndpoint] = server.cluster[cluster]['tables']['state'][tableEndpoint]
                    if tableEndpoint in server.cluster[cluster]['tables']['state']:
                        server.cluster[cluster]['tables'][table]['endpoints'][tableEndpoint] = server.cluster[cluster]['tables']['state'][tableEndpoint]
                    continue
                if server.cluster[cluster]['tables'][table]['endpoints'][tableEndpoint]['inSync'] == True:
                    tableEndpoints['inSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
                else:
                    tableEndpoints['outOfSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
        else:
            print("get_table_endpoints This should never happen")
            tableEndpoints['inSync']['temp'] =  {'path': os.environ['PYQL_CLUSTER_SVC']}
        print(f"get_table_endpoints finished for table {table}: status {tableEndpoints}")
        return tableEndpoints

    def get_db_name(cluster, endpoint):
        if cluster == 'pyql':
            return 'cluster'
        for database in server.cluster[cluster]['databases']:
            if endpoint == server.cluster[cluster]['databases'][database]['endpoint']:
                return database.split(f'{endpoint}_')[1]
        #print(f"f {cluster} dbs: {server.cluster[cluster]['databases']}")
        assert False, f"No DB found with {cluster} endpoint {endpoint}"

    def get_endpoint_url(cluster, endpoint, db, table, action, **kw):
        #if not cluster in server.cluster:
        #    update_cluster(cluster)
        endPointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
        if 'commit' in kw:
            #/db/<database>/cache/<table>/txn/<action>
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
            print(f'table endpoints for updating {table}/{conf}/{action} {tableEndpoints}')
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
            return {"message": str(repr(e)) + f"missing input for {cluster} {table} {action}"}, 400
        print(f"post_request_tables data {requestData}")
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
                    print(f"unable to {action} from endpoint {endpoint} using {requestData}")
                    error, rc = asyncResults[endpoint]['content'], asyncResults[endpoint]['status']
                else:
                    endpointResponse[endpoint] = asyncResults[endpoint]['content']
                    response, rc = asyncResults[endpoint]['status'], asyncResults[endpoint]['status']
            
            
            # At least 1 success in endpoint db change, need to mark failed endpoints out of sync
            # and create a changelog for resync
            if len(failTrack) > 0 and len(failTrack) < len(tableEndpoints['inSync']):
                print("At least 1 successful response & at least 1 failure to update of inSync endpoints")
                print(f"failTrack - {failTrack}")

                # update state for outOfSyncNode
                statusData = {failedEndpoint: {'inSync': False} for failedEndpoint in failTrack}
                mesage, rc = post_update_table_conf(cluster, table,'sync','status', statusData)
                
                if not rc == 200:
                    message = f"canceling request, - quorum {server.quorum['nodes']} inQuorum {server.quorum['status']}"
                    #TODO - need to rollback and cancel cached requests for inSync & not failTracked endpoints
                    print(message)
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
                print(f"All endpoints failed request {failTrack} {error} {rc} using {requestData} thus will not update logs")
                return {"message": error}, rc if rc is not None else r.status_code
            else:
                # No InSync failures
                pass
            # Update any previous out of sync table change-logs, if any
            
            for outOfSyncEndpoint in tableEndpoints['outOfSync']:
                tbEndpoint = f'{outOfSyncEndpoint}{table}'
                print(f"post_request_tables outOfSyncEndpoint:{tbEndpoint}")
                if not tbEndpoint in tb['endpoints'] or not 'state' in tb['endpoints'][tbEndpoint]:
                    print(f"outOfSyncEndpoint {tbEndpoint} may be new, not triggering resync yet {tb['endpoints']}")
                    continue
                if not tb['endpoints'][tbEndpoint]['state'] == 'new':
                    # Check duration of outOfSync, after GracePeriod, stop generating logs
                    # 10 minute timeout
                    #lagTime = time.time() - server.cluster[cluster]['tables']['state'][tbEndpoint]['lastModTime']
                    #print(f"outOfSyncEndpoint {tbEndpoint} lag time is {lagTime}")
                    #if lagTime > 600.0:
                    #    print(f"outOfSyncEndpoint {tbEndpoint} lag time is greater than 600.0, skipping updating logs")
                    #    continue
                    # Retry table resync
                    """ TODO - reenable once cluster is bootstrapped
                    if lagTime > 300.0:
                        #TODO Create job for resync if failed
                        print(f"outOfSyncEndpoint {tbEndpoint} lag time is greater than 300.0, creating another job to resync")
                        server.jobs.append({
                            'job': 'sync_table',
                            'jobType': 'tablesync',
                            'cluster': cluster,
                            'table': table
                        })
                    """
                    # Prevent writing transaction logs for failed transaction log changes
                    if cluster == 'pyql':
                        if table == 'transactions':
                            continue
                    print(f"new outOfSyncEndpoint {tbEndpoint} need to write to db logs")
                    changeLogs['txns'].append({
                            'endpoint': tb['endpoints'][tbEndpoint]['uuid'],
                            'uuid': requestUuid,
                            'tableName': table,
                            'cluster': cluster,
                            'timestamp': transTime,
                            'txn': {action: requestData}
                        })
                else:
                    print(f"post_request_tables  table is new {tb['endpoints'][tbEndpoint]['state']}")
            

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
            print(f"commiting {action} in inSync to endpoints {endpointResponse}")
            print(epCommitRequests)
            asyncResults = asyncrequest.async_request(epCommitRequests, 'POST')
            return {"message": asyncResults}, 200
        if tb['isPaused'] == False:
            return process_request()
        else:
            print(f"Table {table} is paused, Waiting 2.5 seconds before retrying")
            time.sleep(2.5)
            #TODO - create a counter stat to track how often this occurs
            if tb['isPaused'] == False:
                return process_request()
            else:
                #TODO - create a counter stat to track how often this occurs
                return {
                    "message": "table is paused preventing changes, maybe an issue occured during sync cutover, try again later"
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
            print(f"cluster table_select {method} - {table} response {r}")
            return r.json(), r.status_code
        except Exception as e:
            print(repr(e))
            error = f"{r.text} {r.status_code}"
            print(f"table_select error {error}")
            return {"data": [], "error": error}, r.status_code


    @server.route('/cluster/<cluster>/table/<table>/select', methods=['GET','POST'])
    def cluster_table_select(cluster, table):
        if request.method == 'GET':
            return table_select(cluster, table)
        else:
            return table_select(cluster, table, request.get_json(), 'POST')

    @server.route('/cluster/<cluster>/table/<table>/update', methods=['POST'])
    def cluster_table_update(cluster, table, data=None):
        print("cluster_table_update {cluster} {table}")
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
        try:
            config = request.get_json()
        except Exception as e:
            print(repr(e))
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
            print(f"##cluster_table_status /cluster/pyql/table/state/state/get input is {config}")
            endpoints = {endpoint: tb['endpoints'][f'{endpoint}{table}'] for endpoint in config['endpoints']}
            return endpoints, 200
        else:
            return {"message": f"invalid action {action} provided"}, 400
    
    @server.route('/cluster/<cluster>/table/<table>/sync/<action>', methods=['GET','POST'])
    def cluster_table_sync(cluster, table, action):
        try:
            config = request.get_json()
        except Exception as e:
            print(repr(e))
        if not cluster in server.cluster or not table in server.cluster[cluster]['tables']:
            update_cluster(cluster)
        tb = server.cluster[cluster]['tables'][table]
        if action == 'status':
            if request.method == 'GET':
                #/sync/status
                #update_cluster(cluster)
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
                    #update_cluster(cluster)
                tb['endpoints'][tableEndpoint]['inSync'] = config[endpoint]['inSync']
            print(f"###cluster_table_sync set {config} for {table}")
            return {"message": f"set {config} for {table}"}, 200
    @server.route('/cluster/<cluster>/table/<table>/pause/<action>', methods=['GET','POST'])
    def cluster_table_pause(cluster, table, action):
        try:
            config = request.get_json()
        except Exception as e:
            print(repr(e))
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
                print(f"get_cluster_table_endpoint_logs GET - non 200 rc encountered {response} {rc}")
                return response, rc
            if action == 'count':
                return {"availableTxns": len(response['data'])}, rc
            elif action == 'getAll':
                print(f"#get_cluster_table_endpoint_logs getAll {response}")
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
                        print("something abnormal happened when commiting txnlog {txn}")
                cleanUpCheck, rc = get_cluster_table_endpoint_logs(cluster, table, endpoint, 'count', GET=True)
                if cleanUpCheck['availableTxns'] > 0:
                    print(f"something abnormal happened during commiting txnlogs {commitedTxns['txns']}")
                    print(f"get_cluster_table_endpoint_logs - cleanUpCheck expected 0, {cleanUpCheck['availableTxns']} found")
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
            print(message)
            print(f"{[r.status_code for r in results]}")
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
            print(f"join cluster for {clusterName}")
            config = request.get_json()
            print(config)
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
                # Add Job to sync new endpoint with others, if any in cluster

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
                # Add Job to sync tables in cluster within this db, if any in cluster

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
                            print(endpointDb)
                            print(f"##join uuid {config['database']['uuid']}")
                            data = {
                                'name': tableEndpoint,
                                'state': loadState,
                                'inSync': syncState,
                                'uuid': endpointDb['uuid'], # used for syncing logs 
                                'lastModTime': 0.0
                            }
                            post_request_tables('pyql', 'state', 'insert', data)
                            #update_state(clusterName, config['name'], table)
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

            # Create Jobs to SYNCing table
            def wait_on_jobs(curInd, jobList):
                if len(jobList) > curInd + 1:
                    jobList[curInd]['nextJob'] = wait_on_jobs(curInd+1, jobList)
                if curInd == 0:
                    return cluster_jobs_add('syncjobs', jobList[curInd])[0]['jobId']
                return cluster_jobs_add('syncjobs', jobList[curInd], status='waiting')[0]['jobId']
                
            if len(jobsToRun) > 0:
                if clusterName == 'pyql':
                    startJobId = wait_on_jobs(0, jobsToRun)
                    print(f"join_cluster finished - job {startJobId} started for pyql cluster table sync")
                else:
                    for job in jobsToRun:
                        server.jobs.append(job)
                    ## Maybe - cluster_jobs_add()
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
                if tb['endpoints'][tableEndpoint]['state'] == 'new':
                    r = requests.post(
                        'http://{endpointPath}/db/{database}/table/create',
                        headers={'Accept': 'application/json', "Content-Type": "application/json"},
                        data=json.dumps(tb['config'])
                    )
                    print(f'{r.message} {r.status_code}')
        if table == None:
            for table in server.cluster[cluster]['tables']:
                table_config_sync(table)
        else:
            table_config_sync(table)
            



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

    def cluster_job_manager(action, queue, job=None):
        if action == 'add' and not job == None:
            jobId = uuid.uuid1()
            inSyncEndpoints = get_table_endpoints('pyql', 'endpoints')['inSync']
            epRequests = {}
            if 'pyql' in server.cluster:
                for endpoint in inSyncEndpoints:
                    endpointPath = server.cluster['pyql']['endpoints'][endpoint]['path']
                    epRequests[endpoint] = {
                        'path': f'http://{endpointPath}{queue}', 
                        'data': {str(jobId): job}
                        }
                print(f"starting cluster_job_manager request to add job to cluster queue {queue} with url {endpointPath}")
                asyncResults = asyncrequest.async_request(epRequests, 'POST')
                return {"message": f"job {job['jobType']} created with id: {jobId}"},200
        else:
            return {"message": "pyql cluster is not ready yet"}, 200
    @server.route('/cluster/jobmgr/<jobType>/<uuid>/<status>', methods=['POST'])
    def post_cluster_job_update_status(jobType, uuid, status, jobInfo=None):
        try:
            jobInfo = request.get_json() if jobInfo == None else jobInfo
        except:
            jobInfo = {}
        inSyncEndpoints = get_table_endpoints('pyql', 'jobs')['inSync']
        print(f"post_cluster_job_update_status inSyncEndpoints {inSyncEndpoints} for job {uuid}")
        #for endpoint in server.cluster['pyql']['endpoints']:
        epRequests = {}
        for endpoint in inSyncEndpoints:
            endpointPath = inSyncEndpoints[endpoint]['path']
            epRequests[endpoint] = {
                    'path': f'http://{endpointPath}/cluster/{jobType}/{uuid}/{status}',
                    'data': jobInfo
            }
        asyncResults = asyncrequest.async_request(epRequests, 'POST')
        """
            r = requests.post(
                f'http://{endpointPath}/cluster/{jobType}/{uuid}/{status}'
            )
        """
        print(f"post_cluster_job_update_status - results {asyncResults}")
        return {
            "message": f"updated {uuid} with status {status}",
            "results": asyncResults
            }, 200
    @server.route('/cluster/jobqueue/<jobtype>', methods=['POST'])
    def cluster_jobqueue(jobtype):
        """
            Used by jobworkers or tablesyncers to pull jobs from clusters job queues
            jobtype = 'job|syncjob'
        """
        queue = f'{jobtype}s'
        node = request.get_json()['node']
        node = '-'.join(node.split('.'))
        """
        Jobs Table
        'jobs', [
           ('id', str, 'UNIQUE'), 
           ('type', str), #tablesync - cluster
           ('status', str), # QUEUED, RUNNING
           ('node', str),
           ('config', str),
           ('start_time', str),
           ('lastError', str)

        """
        if not node in get_table_endpoints('pyql', 'jobs')['inSync']:
            print(f"{node} is not inSync with pyql cluster yet, cannot pull job")
            return {"message": f"{node} is not inSync with pyql cluster yet, cannot pull job"}, 200
        while True:
            jobSelect = {
                'select': ['id'], 
                'where':{
                    'status': 'queued',
                    'type': queue,
                    'node': None
                }
            }   
            jobList, rc = table_select('pyql', 'jobs', jobSelect, 'POST')
            jobList = jobList['data']
            print(f"joblist: {jobList}")

            if not len(jobList) > 0:
                print(f"queue {queue} - no jobs to process at this time")
                return {"message": f"no jobs to process at this time"}, 200 

            latest = 3 if len(jobList) >= 3 else len(jobList)
            jobIndex = randrange(latest-1) if latest -1 > 0 else 0

            job = jobList[jobIndex]
            print(f"job: {job}")
            jobSelect['where']['id'] = job['id']
            # Attempt to reserve job if no other node has taken
            jobUpdate = {'set': {'node': node}, 'where': {'id': job['id'], 'node': None}}
            post_request_tables('pyql', 'jobs', 'update', jobUpdate)

            # verify if job was reserved by node and pull config
            jobSelect['where']['node'] = node
            jobSelect['select'] = ['*']
            jobCheck, rc = table_select('pyql', 'jobs', jobSelect, 'POST')
            print(f"jobCheck {jobCheck}")
            if not len(jobCheck['data']) > 0:
                continue
            print(f"cluster_jobqueue - pulled job {jobCheck['data'][0]}")
            return jobCheck['data'][0], 200

    @server.route('/cluster/<jobtype>/<uuid>/<status>', methods=['POST'])
    def cluster_job_update(jobtype, uuid, status):
        try:
            jobInfo = request.get_json()
        except:
            jobInfo = {}
        if status == 'finished':
            deleteFrom = {'where': {'id': uuid}}
            return post_request_tables('pyql', 'jobs', 'delete', deleteFrom)
        if status == 'running' or status == 'queued':
            updateSet = {'lastError': {}, 'status': status}
            for k,v in jobInfo.items():
                if k =='start_time' or k == 'status':
                    updateSet[k] = v
                    continue
                updateSet['lastError'][k] = v
            updateSet['lastError'] = updateSet['lastError']
            updateWhere = {'set': updateSet, 'where': {'id': uuid}}
            if status =='queued':
                updateWhere['set']['node'] = None
            return post_request_tables('pyql', 'jobs', 'update', updateWhere)

    @server.route('/cluster/syncjobs', methods=['GET','POST'])
    def cluster_syncjobs():
        """
            used for adding jobs to clusterJobs queue.
        """
        if request.method == 'POST':
            job = request.get_json()
            #print(job)
            for uuid in job:
                jobType = job[uuid]['jobType']
                server.clusterJobs[uuid]=job[uuid]
                server.clusterJobs[uuid]['jobId'] = uuid
                server.clusterJobs['syncjobs'].append(uuid)
            return {"message": "job {uuid} added"}, 200
        else:
            jobsQueue = table_select('pyql', 'jobs')
            return {'jobs': jobsQueue}, 200

    
    @server.route('/cluster/jobs', methods=['GET','POST'])
    def cluster_jobs():
        """
            used for adding jobs to clusterJobs queue.
        """
        if request.method == 'POST':
            job = request.get_json()
            print(job)
            print(type(job))
            for uuid in job:
                print(job[uuid])
                jobType = job[uuid]['jobType']
                server.clusterJobs[uuid]=job[uuid]
                server.clusterJobs[uuid]['jobId'] = uuid
                server.clusterJobs['jobs'].append(uuid)
                print(f"job {jobType} added with id: {uuid}")
            return {"message": "job added"}, 200
        else:
            jobsQueue,rc = table_select('pyql', 'jobs')
            return {'jobs': jobsQueue}, 200
        

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
        print("cluster_jobs_add called")

        #print(request.get_json())
        job = request.get_json() if job == None else job
        jobId = f'{uuid.uuid1()}'

        jobInsert = {
            'id': jobId,
            'type': jobtype,
            'status': 'queued' if not 'status' in kw else kw['status'],
            'config': job
        }
        post_request_tables('pyql', 'jobs', 'insert', jobInsert)

        return {
            "message": f"job {job} added to jobs queue",
            "jobId": jobId}, 200
    
    @server.route('/clusters/cron/job/add', methods=['POST'])
    def add_cron_job():
        """
            Expected input:
        {
        'name': 'updateState_cron'
        'job': {
            'job': 'updateState',
            'jobType': 'cluster',
            'method': 'POST'
            'path': '/clusters/updateAll'
        },
        'interval': 30.0,
        'lastRunTime': time.time(),
        'status': 'queued'
        }
        """
        cron = request.get_json()
        server.cronJobs[cron['name']] = cron
    

    def cron_job_update_all(cron, data=None):
        config = request.get_json() if data == None else data
        endpoints = get_table_endpoints('pyql', 'state')['inSync']
        for endpoint in endpoints:
            r = request.post(f"{endpoint['path']}/clusters/cron/job/{cron}/update", data=config)
        return r.message, r.status_code

    @server.route('/clusters/cron/job/<cron>/<action>', methods=['POST'])
    def cron_job_action(cron, action):
        if action == 'update':
            if cron in server.cronJobs:
                config = request.get_json()
                for key,value in config.items():
                    if key in server.cronJobs[cron]:
                        server.cronJobs[cron][key] = value
                return {"message": f"updated {cron} with {config}"}, 200 
        elif action == 'updateAll':
            config = request.get_json()
            endpoints = get_table_endpoints('pyql', 'state')['inSync']
            for endpoint in endpoints:
                r = request.post(f"{endpoint['path']}/clusters/cron/job/{cron}/update", data=config)
            return r.message, r.status_code
        else:
            pass

    @server.route('/clusters/cron/job')
    def cron_job():
        queuedJobs = {}
        for cron in server.cronJobs:
            job = server.cronJobs[cron]
            if not job['status'] == 'RUNNING':
                if time.time() - job['lastRunTime'] > job['interval']:
                    cron_job_update_all(
                        job['name'], 
                        {
                            'status': 'RUNNING',
                            'lastRunTime': time.time()
                        })
                    cron_job_update_all(
                        job['name'], 
                        {
                            'status': 'queued',
                            'lastRunTime': time.time()
                        })
                    server.jobs.append(job['job'])
                    return {"message": f"started job {job['name']}"},200
                else:
                    queuedTime = time.time() - job['lastRunTime']
                    queuedJobs[job['name']] = f"starts in {job['interval'] - queuedTime} seconds"
        return {"message": f"no jobs to start", 'queuedJobs': queuedJobs}, 200

    update_clusters()
    server.jobs.append(joinClusterJob)

