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
    os.environ['HOSTNAME'] = 'dev-server02'
    joinClusterJob = {
        "job": "joinCluster",
        "jobType": "node",
        "method": "POST",
        "path": "/cluster/pyql/join",
        "data": {
            "name": os.environ['HOSTNAME'],
            "path": os.environ['PYQL_CLUSTER_SVC'],
            "database": {
                'name': "cluster",
                'uuid': dbuuid
            },
            "tables": [
                {
                    "clusters": server.get_table_func('cluster', 'clusters')
                },
                {
                    "endpoints": server.get_table_func('cluster', 'endpoints')
                },
                {
                    "databases": server.get_table_func('cluster', 'databases')
                },
                {
                    "tables": server.get_table_func('cluster', 'tables')
                },
                {
                    "state": server.get_table_func('cluster', 'state')
                },
                {
                    "transactions": server.get_table_func('cluster', 'transactions')
                }
            ]
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
                'config': server.get_table_func(config['database']['name'], table),
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
                cfg = cfg[0] if type(cfg) == list else json.loads("{config: " + str(cfg) + "}")
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
            epResults = asyncrequest.async_request(epRequests)
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
            return {'message':'OK'}, 200

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
            Updates the in-memory table configuration
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
                        stateIndex = ind
                tables.pop(stateIndex)
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
        print(f"get_table_endpoints finished for {tableEndpoints}")
        return tableEndpoints

    def get_db_name(cluster, endpoint):
        for database in server.cluster[cluster]['databases']:
            if endpoint == server.cluster[cluster]['databases'][database]['endpoint']:
                return database.split(f'{endpoint}_')[1]
        #print(f"f {cluster} dbs: {server.cluster[cluster]['databases']}")
        assert False, f"No DB found with {cluster} endpoint {endpoint}"

    def get_endpoint_url(cluster, endpoint, db, table, action, **kw):
        endPointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
        if 'commit' in kw:
            #/db/<database>/cache/<table>/txn/<action>
            return f'http://{endPointPath}/db/{db}/cache/{table}/txn/commit'
        if 'cache' in kw:
            return f'http://{endPointPath}/db/{db}/cache/{table}/{action}/{kw["cache"]}'
        else:
            return f'http://{endPointPath}/db/{db}/table/{table}/{action}'

    def post_write_to_change_logs(cluster, table, data=None):
        print(f"###post_write_to_change_logs isued for {cluster} {table} {data}")
        """
            Expected input: 
            data = {
                'txns':[
                    {
                        'endpoint': 'uuid',
                        'txnUuid': 'txnuuid-xxdk--dfdkd', 
                        'txn': {
                            'update': {
                                'set': {'name': 'new'}, 
                                'where': {'key': 1}
                            }
                        }
                    },
                    ....
                ]
            }
        """
        if server.quorum == False:
            return f"quorum was not available for writing {cluster} {table} changelog", 500
        tableEndpoints = get_table_endpoints('pyql', 'tables')
        data = request.get_json() if data == None else data
        epRequests = {}
        results = {}
        for endpoint in tableEndpoints['inSync']:
            endPointPath = tableEndpoints['inSync'][endpoint]['path']
            epRequests[endpoint] = {'path': endPointPath, 'data': data}
        asyncResults = asyncrequest.async_request(epRequests, 'POST')
        """
            r = requests.post(
                f'http://{endPointPath}/cluster/{cluster}/tablelogs/{table}',
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                data=json.dumps(data)
            )
        """
        for endpoint in tableEndpoints['inSync']:
            aResult = asyncResults[endpoint]
            try:
                results[endpoint] = {'message': aResult['content'], 'status': aResult['status']}
            except Exception as e:
                results[endpoint] = {'message': aResult['content'], 'status': aResult['status'], 'error': str(repr(e))}
        print(f"###post_write_to_change_logs finished for {cluster} {table} {r.text} {r.status_code}")
        return results, 200

    @server.route('/cluster/<cluster>/tableconf/<table>/<conf>/<action>', methods=['POST'])
    def post_update_table_conf(cluster, table, conf, action, data=None):
        """
            current primary use is for updating sync status of a table & triggering db 
        """
        if server.quorum['status'] == False:
            return {
                "message": f"cluster pyql is not in quorum",
                "error": f"quorum was not available"}, 500

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
                job = {
                    'job': 'updateTableSyncStatus',
                    'jobType': 'cluster',
                    'method': 'POST',
                    'path': f'/cluster/{cluster}/table/{table}/update',
                    'data': update,
                    'runAfter': updateStateJob
                }
                jobsToRun.append(job)

        tableEndpoints = get_table_endpoints('pyql', 'state')
        epRequests = {}
        for endpoint in tableEndpoints['inSync']:
            total+=1
            endPointPath = f'{endpoint["path"]}/cluster/{cluster}/table/{table}/{conf}/{action}'
            epRequests[endpoint] = {'path': endPointPath, 'data': json.dumps(data)}
        asyncResults = asyncrequest.async_request(epRequests, 'POST')
        """
            r = requests.post(
                f'{endpoint['path']}/cluster/{cluster}/table/{table}/{conf}/{action}',
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                data=json.dumps(data)
            )
        """
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
            requestUuid = uuid.uuid1()
            for endpoint in tableEndpoints['inSync']:
                tb = server.cluster[cluster]['tables'][table]
                db = get_db_name(cluster, endpoint)
                epRequests[endpoint] = {
                    'path': get_endpoint_url(cluster, endpoint, db, table, action, cache=requestUuid),
                    'data': json.dumps(requestData)
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
            def update_endpoint_time(override=False): 
                # Update endpoint lastModTime
                tableEndpoints = get_table_endpoints(cluster, table)
                currentTime = time.time()
                for endpoint in tableEndpoints['inSync']:
                    tableEndpoint=f'{endpoint}{table}'
                    if endpoint in failTrack:
                        #Endpoint is not in sync
                        continue
                    if currentTime - tb['endpoints'][tableEndpoint]['lastModTime'] > 60.0 or override:
                        timeData = {
                            'set': {
                                'lastModTime': currentTime
                                }, 
                            'where': {
                                'name': f'{endpoint}{table}'
                            }
                        }
                        updateStateJob = {
                            'job': 'updateTableState',
                            'jobType': 'cluster',
                            'method': 'POST',
                            'path': f'/cluster/{cluster}/endpoint/{endpoint}/db/{get_db_name(cluster, endpoint)}/table/{table}/update'
                        }
                        job = {
                            'job': 'updateTableModTime',
                            'method': 'POST',
                            'jobType': 'cluster',
                            'path': '/cluster/pyql/table/state/update',
                            'data': timeData,
                            'runAfter': updateStateJob
                        }
                        #post_request_tables(cluster, 'state', 'update', data)
                        #server.jobs.append(job) #TODO update later
            transTime = time.time()
            # At least 1 success in endpoint db change, need to mark failed endpoints out of sync
            # and create a changelog for resync
            if len(failTrack) > 0 and len(failTrack) < len(tableEndpoints['inSync']):
                # At least 1 successful response & at least 1 failure to update of inSync endpoints
                statusData = {failedEndpoint: {'inSync': False} for failedEndpoint in failTrack}
                mesage, rc = post_update_table_conf(cluster, table,'sync','status', statusData)
                
                if not rc == 200:
                    message = f"canceling request, internal error preventing sync status update on each cluster node"
                    #TODO - need to rollback and cancel cached requests for inSync & not failTracked endpoints
                    return message, 500

                for failedEndpoint in failTrack:
                    if not tb['endpoints'][failedEndpoint]['state'] == 'new':
                        endPointPath = server.cluster[cluster]['endpoints'][failedEndpoint]['path']
                        # update sync status for table endpoint as outOfSync
                        #TODO - Check later if we can jobify this
                        print(f"need to update state on {cluster} {table} {statusData}")

                        """
                        At this point in the code, at least 1 inSync endpoint was successful for stating the table change &
                        at least 1 failed. We must mark this failed endpoint as inSync=False on each pyql cluster node 
                        state tables to prevent both change or select requests now that the endpoint is out of sync.
                        1. Set table sync status false for in-memory config
                            a. Need to also ensure requests to refresh memory are ignored by using an in-memory value 
                            b. 
                        2. Create cluster job to update state tables on each cluster node, if quorum was available for in-memory update.
                            a. After state is updated within pyql cluster for outOfSync node, issue a commit for staged transaction on InSync nodes
                        3. If quorum was not available for table state, cancel transaction as table state could not be updated by enough clusters 
                           # This would be special case because both table endpoint & pyql node were not reachable, we cannot commit the successful 
                             staged table changes, as not enough pyql state endpoints could verify this node is NOT the problematic entity.
                             Think pyql-master-node & db table endpoint (successful) shared the same subnet, but other 2 + pyql-master-nodes were not 
                             reachable from this node nor were any other table endpoints. Thus this transaction is occuring on the stale or problematic 
                             node and cannot be consider consistent and marked stale OutOfSync until connectivity issues are resolved & resynced.
                        """

                            
                        #TODO - maybe create job to directly update pyql state table for this failedEndpoint
                        #  and then update in-memory endpoint used by get_table_endpoints() so future requests avoid it
                        #TODO - need special handling for PYQL cluster state table as this table requires quorum for normal use.
                        #TODO - maybe use special queue for writing transaction logs to each cluster node, avoid blocking.
                        
                        # Prevent writing transaction logs for failed transaction log changes
                        if cluster == 'pyql':
                            if table == 'transactions':
                                continue

                        # Write data to a change log for resyncing
                        changeLogs['txns'].append({
                                'endpoint': tb['endpoints'][failedEndpoint]['uuid'],
                                'uuid': requestUuid,
                                'table': table,
                                'cluster': cluster,
                                'timestamp': transTime,
                                'txn': {action: requestData}
                            })
                        #TODO Create job for resync if failed

                        server.jobs.append({
                            'job': 'sync_table',
                            'jobType': 'tablesync',
                            'cluster': cluster,
                            'table': table
                        })
                 # At least 1 new sync failure - creating job to modtime on inSync table endpoints
                #update_endpoint_time(True) #TODO Check if I need this still

            # All endpoints failed request - 
            elif len(failTrack) == len(tableEndpoints['inSync']):
                print(f"All endpoints failed request {failTrack} {error} {rc} using {requestData} thus will not update logs")
                return error, rc if rc is not None else r.status_code
            else:
                # No InSync failures
                print(f"post_request_tables no inSync failures")
                #update_endpoint_time()#TODO Check if I need this still
                pass
            # Update any previous out of sync table change-logs, if any
            print(f"post_request_tables need to update logs of any outOfSync endpoinsts {tableEndpoints['outOfSync']} {tb}")
            for outOfSyncEndpoint in tableEndpoints['outOfSync']:
                tbEndpoint = f'{outOfSyncEndpoint}{table}'
                print(f"post_request_tables outOfSyncEndpoint:{tbEndpoint}")
                if not tbEndpoint in tb['endpoints'] or not 'state' in tb['endpoints'][tbEndpoint]:
                    print(f"outOfSyncEndpoint {tbEndpoint} may be new, not triggering resync yet {tb['endpoints']}")
                    continue
                if not tb['endpoints'][tbEndpoint]['state'] == 'new':
                    # Check duration of outOfSync, after GracePeriod, stop generating logs
                    # 10 minute timeout
                    lagTime = time.time() - server.cluster[cluster]['tables']['state'][tbEndpoint]['lastModTime']
                    print(f"outOfSyncEndpoint {tbEndpoint} lag time is {lagTime}")
                    #if lagTime > 600.0:
                    #    print(f"outOfSyncEndpoint {tbEndpoint} lag time is greater than 600.0, skipping updating logs")
                    #    continue
                    # Retry table resync
                    if lagTime > 300.0:
                        #TODO Create job for resync if failed
                        print(f"outOfSyncEndpoint {tbEndpoint} lag time is greater than 300.0, creating another job to resync")
                        server.jobs.append({
                            'job': 'sync_table',
                            'jobType': 'tablesync',
                            'cluster': cluster,
                            'table': table
                        })
                    #TODO - Check later if we can jobify this
                    # Prevent writing transaction logs for failed transaction log changes
                    if cluster == 'pyql':
                        if table == 'transactions':
                            continue
                    print(f"outOfSyncEndpoint {tbEndpoint} need to write to db logs")
                    changeLogs['txns'].append({
                            'endpoint': tb['endpoints'][tbEndpoint]['uuid'],
                            'uuid': requestUuid,
                            'table': table,
                            'cluster': cluster,
                            'timestamp': transTime,
                            'txn': {action: requestData}
                        })
                else:
                    print(f"post_request_tables  table is new {tb['endpoints'][tbEndpoint]['state']}")
            

            details = {'message': response, 'endpointStatus': tb['endpoints']} if 'details' in kw else response
            if len(changeLogs['txns']) > 0:
                """
                #TODO - Delete this later
                result, rc = post_write_to_change_logs(
                    cluster,
                    table,
                    changeLogs
                )
                """
                # Better solution - maintain transactions table for transactoins, table sync and logic is already available
                for txn in changeLogs['txns']:
                    post_request_tables('pyql','transactions','insert', txn)

            # Commit cached commands  
            epCommitRequests = {}
            for endpoint in endpointResponse:
                db = get_db_name(cluster, endpoint)
                epRequests[endpoint] = {
                    'path': get_endpoint_url(cluster, endpoint, db, table, action, commit=True),
                    'data': json.dumps(endpointResponse[endpoint])
                }
            asyncResults = asyncrequest.async_request(epCommitRequests, 'POST')
            return response, 200
        if tb['isPaused'] == False:
            print(f"table {table} is not paused, processing")
            return process_request()
        else:
            print(f"Table {table} is paused, Waiting 2.5 seconds before retrying")
            time.sleep(2.5)
            if tb['isPaused'] == False:
                return process_request()
            else:
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
        return r.json(), r.status_code


    @server.route('/cluster/<cluster>/table/<table>/select', methods=['GET','POST'])
    def cluster_table_select(cluster, table):
        if request.method == 'GET':
            return table_select(cluster, table)
        else:
            return table_select(cluster, table, request.get_json(), 'POST')

    @server.route('/cluster/<cluster>/table/<table>/update', methods=['POST'])
    def cluster_table_update(cluster, table):
        print("cluster_table_update")
        return post_request_tables(cluster, table, 'update', request.get_json())
            
    @server.route('/cluster/<cluster>/table/<table>/insert', methods=['POST'])
    def cluster_table_insert(cluster, table):
        return post_request_tables(cluster, table, 'insert',  request.get_json())

    @server.route('/cluster/<cluster>/table/<table>/delete', methods=['POST'])
    def cluster_table_delete(cluster, table):
        return post_request_tables(cluster, table, 'delete', request.get_json())

    @server.route('/cluster/<cluster>/table/<table>/<conf>/<action>', methods=['GET','POST'])
    def cluster_table_status(cluster, table, conf, action='status'):
        timeStart = time.time()
        print(f"starting cluster_table_status")
        tb = server.cluster[cluster]['tables'][table]
        if request.method == 'POST':
            config = request.get_json()
            if conf == 'state':
                if action == 'set':
                    """
                        Expected  input {endpoint: {'state': 'new|loaded'}}
                    """
                    for endpoint in config:
                        tb['endpoints'][endpoint]['state'] = config[endpoint]['state']
                    return {"message": f"set {config} for {table}"}
                elif action == 'get':
                    """
                        Expected  input {'endpoints': ['endpoint1', 'endpoint2']}
                    """
                    print(f"##cluster_table_status /cluster/pyql/table/state/state/get input is {config}")
                    endpoints = {endpoint: tb['endpoints'][f'{endpoint}{table}'] for endpoint in config['endpoints']}
                    return endpoints, 200
                else:
                    pass

            elif conf == 'sync':
                if action == 'status':
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
                    print(f"###cluster_table_status set {config} for {table}")
                    return {"message": f"set {config} for {table}"}
            elif conf == 'pause':
                if 'action' == 'start':
                    tb['isPaused'] == True
                elif 'action' == 'stop':
                    tb['isPaused'] == False
                elif 'action' == 'status':
                    tb['isPaused'] == config['isPaused']
                else:
                    pass
                return {'isPaused': tb['isPaused']}, 200
        # GET Methods
        else:
            if action == 'status':
                if conf == 'sync':
                    #endpoints = {endpoint: tb['endpoints'][endpoint] for endpoint in tb['endpoints']}
                    #response = {'endpoints': endpoints, 'isPaused': tb['isPaused']}
                    update_cluster(cluster)
                    response = get_table_endpoints(cluster, table)
                    return response, 200
                elif conf == 'pause':
                    return {'isPaused': tb['isPaused']}, 200
        print(f"finished cluster_table_status in {time.time()-timeStart}")
        return {"message": f'{conf}/{action} is not a valid for tables'}, 400

    @server.route('/cluster/<cluster>/tablelogs/<table>', methods=['POST'])
    def cluster_table_endpoint_logs(cluster, table):
        if request.method == 'POST':
            """
                Expected input: 
                data = {
                    'txns':[
                        {
                            'endpoint': tb['endpoints'][tbEndpoint]['uuid'],
                            'uuid': requestUuid,
                            'timestamp': transTime,
                            'txn': {action: requestData }
                        },
                        ....
                    ]
                }
            """
            newLogs = request.get_json()
            

            responses = []
            errors = []
            for newLog in newLogs['txns']:
                try:
                    logFile = f'{cluster}_{table}_{newLog["endpoint"]}_tx.logs'
                    with open(logFile,'a+') as changeLogs:
                        changeLogs.write(f'{json.dumps(newLog)}\n')
                        responses.append({'total logs': len([i for i,v in enumerate(changeLogs)])})
                except Exception as e:
                    print(repr(e))
                    errors.append(str(repr(e)))
            return {"messages": responses, 'errors': errors}, 200
    @server.route(f'/cluster/<cluster>/tablelogs/<table>/<endpoint>/<action>', methods=['GET','POST'])
    def get_cluster_table_endpoint_logs(cluster, table, endpoint, action):
        if request.method == 'GET':
            if action == 'count':
                selQuery = ['uuid']
            if action == 'getAll':
                selQuery = ['*']
            clusterTableEndpointTxns = {
                'select': selQuery,
                'where': {
                    'endpoint': endpoint,
                    'cluster': cluster,
                    'table': table
                    }
            }
            response, rc = table_select(
                cluster, 
                table, 
                clusterTableEndpointTxns,
                'POST'
                )
            if not rc == 200:
                return response, rc
            if action == 'count':
                return {"availableTxns": len(response)}, rc
            elif actoin == 'getAll':
                return {'txns': response}, rc
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
                            'table': table,
                            'uuid': txn
                        }
                    }
                    resp, rc = post_request_tables(cluster, table, 'delete', deleteTxn)
                    if not rc == 200:
                        print("something abnormal happened when commiting txnlog {txn}")
                cleanUpCheck, rc = get_cluster_table_endpoint_logs(cluster, table, endpoint, 'count')
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
            for endpoint in endpoints:
                r = request.post(f'{endpoint["path"]}/clusters/update')
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
                                #Table arleady existed in cluster, but endpoint with same table was added
                                syncState = False
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
            #TODO add as global jobs table, so work can be split up among other nodes
            for job in jobsToRun:
                server.jobs.append(job)
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
                        data=json.loads(tb['config'])
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
            for endpoint in inSyncEndpoints:
                endpointPath = server.cluster['pyql']['endpoints'][endpoint]['path']
                epRequests[endpoint] = {
                    'path': endpointPath, 
                    'data': json.dumps({str(jobId): job})
                    }
            print(f"starting cluster_job_manager request to add job to cluster queue {queue} with url {endpointPath}")
            asyncResults = asyncrequest.async_request(epRequests, 'POST')
            """
                r = requests.post(
                    f'http://{endpointPath}{queue}',
                    headers={'Accept': 'application/json', "Content-Type": "application/json"},
                    data = json.dumps({str(jobId): job})
                    )
                print("finished cluster_job_manager request to add job to cluster queue")
                print(f"{r.text} {r.status_code}"
            """
            return {"message": f"job {job['jobType']} created with id: {jobId}"},200
    def post_cluster_job_update_status(jobType, uuid, status):
        inSyncEndpoints = get_table_endpoints('pyql', 'endpoints')['inSync']
        print(f"post_cluster_job_update_status inSyncEndpoints {inSyncEndpoints}")
        #for endpoint in server.cluster['pyql']['endpoints']:
        epRequests = {}
        for endpoint in inSyncEndpoints:
            endpointPath = inSyncEndpoints[endpoint]['path']
            epRequests[endpoint] = {
                    'path': endpointPath
            }
        asyncResults = asyncrequest.async_request(epRequests, 'POST')
        """
            r = requests.post(
                f'http://{endpointPath}/cluster/{jobType}/{uuid}/{status}'
            )
        """
        return {"message": f"updated {uuid} with status {status}"}, 200
    @server.route('/cluster/jobqueue/<jobtype>')
    def cluster_syncjob(jobtype):
        """
            Used by jobworkers or tablesyncers to pull jobs from clusters job queues
            jobtype = 'job|syncjob'
        """
        queue = f'{jobtype}s'
        if len(server.clusterJobs[queue]) > 0:
            uuid = server.clusterJobs[queue].pop(0)
            post_cluster_job_update_status(jobtype, uuid, 'running')
            return server.clusterJobs[uuid], 200
        else:
            return {"message": f"no jobs to process at this time"}, 200

    @server.route('/cluster/<jobtype>/<uuid>/<status>', methods=['POST'])
    def cluster_job_update(jobtype, uuid, status):
        if status == 'running':
            if uuid in server.clusterJobs[f'{jobtype}s']:
                index = server.clusterJobs[f'{jobtype}s'].index(uuid)
                server.clusterJobs[f'{jobtype}s'].pop(index)
            if uuid in server.clusterJobs:
                server.clusterJobs[uuid]['status'] = status
            return {"message": f"updated {jobtype} id {uuid} with status {status}"}, 200
        """

            if jobtype == 'job':
                if uuid in server.clusterJobs['jobs']:
                    index = server.clusterJobs['jobs'].index(uuid)
                    server.clusterJobs['jobs'].pop(index)
                if uuid in server.clusterJobs:
                    server.clusterJobs[uuid]['status'] = status
            elif jobtype == 'syncjobs':
                if uuid in server.clusterJobs['syncjobs']:
                    index = server.clusterJobs['syncjobs'].index(uuid)
                    server.clusterJobs['jobs'].pop(index)
                if uuid in server.clusterJobs:
                    server.clusterJobs[uuid]['status'] = status
            else:
        """
                
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
                server.clusterJobs['syncjobs'].append(uuid)
            return {"message": "job added"}, 200
        else:
            return server.clusterJobs, 200

    
    @server.route('/cluster/jobs', methods=['GET','POST'])
    def cluster_jobs():
        """
            used for adding jobs to clusterJobs queue.
        """
        if request.method == 'POST':
            job = request.get_json()
            #print(job)
            for uuid in job:
                jobType = job[uuid]['jobType']
                server.clusterJobs[uuid]=job[uuid]
                server.clusterJobs['jobs'].append(uuid)
                print(f"job {jobType} added with id: {uuid}")
            return {"message": "job added"}, 200
        else:
            return server.clusterJobs, 200
        

    @server.route('/cluster/<jobtype>/add', methods=['POST'])
    def cluster_jobs_add(jobtype):
        """
            meant to be used by node workers which will load jobs into cluster job queue
            to avoiding delays from locking during change operations
            For Example:
            # Load a job into node job queue
            server.jobs.append({'job': 'job-name', ...})
        """
        print("cluster_jobs_add called")

        #print(request.get_json())
        job = request.get_json()
        if job['jobType'] == 'tablesync':
            print(f'###tablesync job ###')
            print(server.cluster['pyql']['tables']['endpoints'])

        if job['jobType'] == 'cluster' or job['jobType'] == 'tablesync':
            cluster_job_manager(
                'add', 
                '/cluster/jobs' if jobtype == 'jobs' else '/cluster/syncjobs',
                job
            )
            return {"message": f"job {job} added to cluster queue"}, 200
        else:
            return {"message": f"job {job} missing 'cluster' key required for running into cluster queue"}, 400
    
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
            if not job['status'] == 'running':
                if time.time() - job['lastRunTime'] > job['interval']:
                    cron_job_update_all(
                        job['name'], 
                        {
                            'status': 'running',
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

