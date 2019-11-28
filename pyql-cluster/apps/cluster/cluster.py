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

TODO: Use JAWF to create tables required for PYQL-CLUSTER function

"""
def run(server):
    from flask import request
    import requests
    from datetime import datetime
    import time, uuid
    from random import randrange
    import json
    server.clusterJobs = {'jobs': []}
    server.cronJobs = {}

    @server.route('/cluster/<cluster>/endpoint/<endpoint>/table/<table>/updateState', methods=['POST'])
    def update_state(cluster, endpoint, table):
        db = server.data['cluster']
        tbEndpoint = f'{endpoint}_{table}'
        state = db.tables['state'].select(
            '*',
            where={
                'cluster': cluster,
                'name': tbEndpoint
                }
        )
        server.cluster[cluster]['state'][tbEndpoint] = state
        return server.cluster[cluster]['state'][tbEndpoint]


    def update_tables(cluster, endpoint, database):
        """
            Updates the in-memory table configuration
        """
        db = server.data['cluster']
        tables = db.tables['tables'].select('*', where={
                        'cluster': cluster, 
                        'database': database
                        )
        for table in tables:
            server.cluster[cluster]['tables'][table['name']] = table
            tb = server.cluster[cluster]['tables'][table['name']]
            if not 'endpoints' in tb:
                tb['endpoints'] = {}
            update_state(cluster, endpoint, table['name'])
            state = server.cluster[cluster]['state'][f'{endpoint}_{table}']
            tb['endpoints'][endpoint] = {
                'inSync': state['inSync'],
                'path': f'{state['path']}/db/{database}/table/{tb['name']}'
            }
                
    def update_databases(cluster, endpoint, tables=True):
        db = server.data['cluster']
        databases = db.tables['databases'].select('*', where={'cluster': cluster, 'endpoint': endpoint['name']})
        for database in databases:
            server.cluster[cluster]['databases'][database['name']] = database
            if tables=True
                update_tables(cluster, endpoint, database)

    def update_endpoints(cluster, databases=True):
        db = server.data['cluster']
        endpoints = db.tables['endpoints'].select('*', where={'cluster': cluster})
        for endpoint in endpoints:
            server.cluster[cluster]['endpoints'][endpoint['name']] = endpoint
            if databases == True:
                update_databases(cluster, endpoint)

    def update_cluster(cluster, endpoints=True):
        if server.cluster is not None:
            db = server.data['cluster']
            server.cluster[cluster] = db.tables['clusters'].select(
                '*',
                where={'name': cluster}
            )
            if endpoints == True:
                update_endpoints(cluster)

    def update_clusters():
        server.cluster= dict()
        db = server.data['cluster']
        clusters = db.tables['clusters'].select('name')
        for cluster in clusters:
            update_cluster(cluster.values()[0])
    def post_update_cluster_config(cluster, action, items, data=None):
        """
            invokes /cluster/<cluster>/config/<action>  on each cluster node
            which triggers update_clusters() on each node refreshing
            in-memory config
        """
        tableEndpoints = get_table_endpoints('pyql', 'clusters')
        data = request.get_json() if data == None else data
        for endpoint in tableEndpoints['inSync']:
            endPointPath = endpoint['path']
            r = request.post(
                f'{endPointPath}/cluster/{cluster}/config/{action}/{items}',
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                data=data
            )        

    def get_endpoint_list(cluster):
        endpointList = [endpoint for endpoint in server.cluster[cluster]['endpoints']]
        return endpointList
    def get_table_endpoints(cluster, table):
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}
        for endpoint in server.cluster[cluster]['endpoints']:
            if server.cluster[cluster]['endpoints'][endpoint]['inSync'] == True:
                tableEndpoints['inSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
            else:
                tableEndpoints['outOfSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
        return tableEndpoints

    def get_db_name(cluster, endpoint):
        for database in server.clusters[cluster]['databases']:
            if endpoint in server.clusters[cluster]['databases'][database]:
                return database
        assert False, f"No DB found with {cluster} {endpoint}"
    def get_endpoint_url(endpoint, db, table, action):
        endPointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
        return f'{endPointPath}/{db}/table/{table}/{action}'

    def post_write_to_change_logs(cluster, table, data=None):
        """
            Expected input: 
            data = {
                    'endpoint': 'uuid', 
                    'txn': {
                        'update': {
                            'set': {'name': 'new'}, 
                            'where': {'key': 1}
                        }
                    }
                }
        """

        tableEndpoints = get_table_endpoints('pyql', 'tables')
        data = request.get_json() if data == None else data
        for endpoint in tableEndpoints['inSync']:
            endPointPath = endpoint['path']
            r = request.post(
                f'{endPointPath}/cluster/{cluster}/tablelogs/{table}',
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                data=data
            )

    def post_update_table_conf(cluster, table, conf, action, data=None):
        endPointList = get_endpoint_list('pyql')
        tableEndpoints = get_table_endpoints('pyql', 'tables')
        data = request.get_json() if data == None else data
        for endpoint in tableEndpoints['inSync']:
            endPointPath = endpoint['path']
            r = request.post(
                f'{endPointPath}/cluster/{cluster}/table/{table}/{conf}/{action}',
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                data=data
            )
    def post_request_tables(cluster, table, action, data=None):
        endPointList = get_endpoint_list(cluster)
        tableEndpoints = get_table_endpoints(cluster, table)
        data = request.get_json() if data == None else data
        failTrack = []
        tb = server.cluster[cluster]['tables'][table]
        def process_request():
            for endpoint in tableEndpoints['inSync']:
                tb = server.cluster[cluster]['tables'][table]
                db = get_db_name(cluster, endpoint)

                def execute_request():
                    return requests.post(
                        get_endpoint_url(endpoint, db, table, action),
                        headers={'Accept': 'application/json', "Content-Type": "application/json"},
                        data=data,
                        timeout=0.5)
                # 2 Retries
                for _ in range(2):
                    r = execute_request()
                    if not r.status_code == 200:
                        continue
                    else:
                        break
                 
                if not r.status_code == 200:
                    failTrack.append(endpoint)
                    # start job to Retry for endpoint, or mark endpoint bad after testing
                    print(f"unable to {action} from endpoint {endpoint}")
                else:
                    response, rc = r.json(), r.status_code
            # At least 1 success in endpoint db change, need to mark failed endpoints out of sync
            # and create a changelog for resync
            def update_endpoint_time(override=False): 
                # Update endpoint lastModTime
                tableEndpoints = get_table_endpoints(cluster, table)
                currentTime = time.time()
                for endpoint in tableEndpoints['inSync']:
                    if endpoint in failTrack:
                        #Endpoint is not in sync
                        continue
                    if currentTime - tb['endpoints'][endpoint]['lastModTime'] > 60.0 or override:
                        data = {
                            'update': {
                                'lastModTime': currentTime
                                }, 
                            'where': {
                                'name': f'{endpoint}_{table}'
                            }
                        }
                        updateStateJob = {
                            'job': 'updateTableState',
                            'jobType': 'cluster',
                            'method': 'POST',
                            'path': f'/cluster/{cluster}/endpoint/{endpoint}/table/{table}/updateState'
                        }
                        job = {
                            'job': 'updateTableModTime',
                            'method': 'POST',
                            'jobType': 'cluster',
                            'path': '/cluster/pyql/table/state/update'
                            'data': data
                            'runAfter': updateStateJob
                        }
                        #post_request_tables(cluster, 'state', 'update', data)
                        server.jobs.append(job)
            if len(failTrack) > 0 and len(failTrack) < len(tableEndpoints['inSync']):
                for failedEndpoint in failTrack:
                    if not tb['endpoints'][failedEndpoint]['state'] == 'new':
                        endPointPath = server.cluster[cluster]['endpoints'][failedEndpoint]['path']

                        # update sync status for table endpoint as outOfSync
                        statusData = {failedEndpoint: {'inSync': False}}
                        #TODO - Check later if we can jobify this
                        post_update_table_conf(cluster, table,'sync','status' statusData)

                        # Write data to a change log for resyncing
                        post_write_to_change_logs(
                            cluster,
                            table, 
                            {
                                'endpoint': tb['endpoints'][failedEndpoint]['uuid'],
                                'txn': {action: data }
                            })
                        #TODO Create job for resync if failed
                        server.jobs.append({
                            'job': 'sync_table',
                            'jobType': 'cluster',
                            'cluster': cluster,
                            'table': table
                        })
                 # At least 1 new sync failure - creating job to modtime on inSync table endpoints
                 update_endpoint_time(True) 

            # All endpoints failed request - 
            elif len(failTrack) == len(tableEndpoints['inSync']):
                return response, rc if rc is not None else r.status_code
            else:
                # No InSync failures
                update_endpoint_time()
                pass
            # Update any previous out of sync table change-logs, if any
            for outOfSyncEndpoint in tableEndpoints['outOfSync']:
                if not server.cluster[cluster]['endpoints'][failedEndpoint]['state'] == 'new':
                    # Check duration of outOfSync, after GracePeriod, stop generating logs
                    tbEndpoint = f'{outOfSyncEndpoint}_{table}'
                    # 10 minute timeout
                    lagTime = time.time() - server.cluster[cluster]['state'][tbEndpoint]['lastModTime']
                    if lagTime > 600.0:
                        continue
                    # Retry table resync
                    if lagTime > 300.0:
                        #TODO Create job for resync if failed
                        server.jobs.append({
                            'job': 'sync_table',
                            'jobType': 'cluster',
                            'cluster': cluster,
                            'table': table
                        })
                    #TODO - Check later if we can jobify this
                    post_write_to_change_logs(
                        cluster,
                        table, 
                        {
                            'endpoint': tb['endpoints'][outOfSyncEndpoint]['uuid'],
                            'txn': {action: data }
                        })

            return response, rc if rc is not None else r.status_code
        if tb['isPaused'] == False:
            return process_request()
        else:
            import time
            print(f"Table {table} is paused, Waiting 2.5 seconds before retrying")
            time.sleep(2.5)
            if tb['isPaused'] == False:
                return process_request()
            else:
                return {
                    "message": "table is paused preventing changes, maybe an issue occured during sync cutover, try again later"
                    }, 500


    @server.route('/cluster/<cluster>/table/<table>/select', methods=['GET','POST'])
    def cluster_table_select(cluster, table):
        if request.method == 'GET':
            #endPointList = get_endpoint_list(cluster)
            endPointList = []
            tableEndpoints = get_table_endpoints(cluster, table)
            for endpoint in tableEndpoints['inSync']:
                endPointList.append(endpoint)
            if len(endPointList) > 1:
                endpoint = endPointList[randrange(len(endPointList))]
                db = get_db_name(cluster, endpoint)
                r = requests.get(get_endpoint_url(endpoint, db, table, 'select'),
                    headers={'Accept': 'application/json'})
                return r.json(), r.status_code
            else:
                return {"status": 500, "message": f"no endpoints found in cluster {cluster}"}
        else:
            endPointList = get_endpoint_list(cluster)
            if len(endPointList) > 1:
                endpoint = endPointList[randrange(len(endPointList))]
                db = get_db_name(cluster, endpoint)
                data = request.get_json()
                r = requests.get(get_endpoint_url(endpoint, db, table, 'select'),
                    headers={'Accept': 'application/json'}, data=data)
                return r.json(), r.status_code
            else:
                return {"status": 500, "message": f"no endpoints found in cluster {cluster}"}

    @server.route('/cluster/<cluster>/table/<table>/update', methods=['POST'])
    def cluster_table_update(cluster, table):
        return post_request_tables(cluster, table, 'update')
            
    @server.route('/cluster/<cluster>/table/<table>/insert', methods=['POST'])
    def cluster_table_insert(cluster, table):
        return post_request_tables(cluster, table, 'insert')

    @server.route('/cluster/<cluster>/table/<table>/delete', methods=['POST'])
    def cluster_table_delete(cluster, table):
        return post_request_tables(cluster, table, 'delete')

    @server.route('/cluster/<cluster>/table/<table>/<conf>/<action>', methods=['GET','POST'])
    def cluster_table_status(cluster, table, conf, action='status'):
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
                    endpoints = {endpoint: tb['endpoints'][endpoint]['state'] for endpoint in config['endpoints']}
                    return endpoints, 200
                else pass

            elif conf == 'sync':
                if action == 'status':
                    # Expected  input {endpoint: {'inSync': False}}
                    for endpoint in config:
                        if config[endpoint]['inSync'] == False:
                            # Create Job for resyncing table endpoint
                        tb['endpoints'][endpoint]['inSync'] = config[endpoint]['inSync']
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
            if action == 'status'
                if conf == 'sync':
                    endpoints = {endpoint: tb['endpoints'][endpoint] for endpoint in tb['endpoints']}
                    response = {'endpoints': endpoints, 'isPaused': tb['isPaused']}
                    return response, 200
                elif conf == 'pause':
                    return {'isPaused': tb['isPaused']}, 200
        return {"message": f'{conf}/{action} is not a valid for tables'}, 400

    @server.route(f'/cluster/<cluster>/tablelogs/<table>', methods=['POST'])
    def cluster_table_endpoint_logs(cluster, table):
        if request.method == 'POST':
            """ EXAMPLE txn
                {
                    'endpoint': 'uuid', 
                    'txn': {
                        'update': {
                            'set': {'name': 'new'}, 
                            'where': {'key': 1}
                        }
                    }
                }
            """
            newLog = request.get_json()
            with open(f'{cluster}_{table}_{newLog['endpoint']}_tx.logs', 'a+') as changeLogs:
                changeLogs.write(f'{json.dumps(newLog)}\n')
                response = {'total logs': len([i for i,v in enumerate(changeLogs)])}
            return response, 200
    @server.route(f'/cluster/<cluster>/tablelogs/<table>/<endpoint>', methods=['POST'])
    def get_cluster_table_endpoint_logs(cluster, table, endpoint):
        """
            {
                'count': 5,
                'startFromIndex': 0 
            }
        """
        logRequest = request.get_json()
        txns = []
        offset = logRequest['startFromIndex']+logRequest['count']-1
        try:
            with open(f'{cluster}_{table}_{endpoint}_tx.logs', 'r') as changeLogs:
                for ind, line in enumerate(changeLogs):
                    if ind >= logRequest['startFromIndex'] and ind <= offset:
                        txns.append(json.loads(line))
            return {
                'totalTxns': len([i for i,v in enumerate(changeLogs)]),
                'requested': logRequest['count'],
                'txns': txns
                }
        except Exception as e:
            print(repr(e))
            message = f'Exception encountered trying to open file: {cluster}_{table}_{endpoint}_tx.logs'
            return message, 500



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
                r = request.post(f'{endpoint['path']}/clusters/update')
        return {"message": f"invalid action {action} for /clusters/"}, 400


    @server.route('/cluster/<cluster>/config/<action>/<items>', methods=['POST'])
    def update_cluster_config(cluster, action, items):
        if action == 'update':
            if items == 'cluster'
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
            if not clusterName in server.cluster:
                #Cluster does not exist, need to create
                #JobIfy - create as job so config is replicated
                data = {
                    'name': clusterName, 
                    'createdByEndpoint': config['name'],
                    'createDate': f'{datetime.now().date()}'
                    }
                post_request_tables(clusterName, 'clusters', 'insert', data)
                update_cluster(cluster, False)
            #check for existing endpoint in cluster: clusterName
            # If endpoint does not exists create
            if not config['name'] in server.cluster[cluster]['endpoints']:
                newEndpointOrDatabase = True
                data = {
                    'name': config['name'],
                    'path': config['path'],
                    'cluster': clusterName
                }
                post_request_tables(clusterName, 'endpoints', 'insert', data)
                update_endpoints(cluster, False)
                # Add Job to sync new endpoint with others, if any in cluster

            # check for existing endpoint db's in cluster: clusterName
            # if db not exist, add
            endpointDatabase = f'{config['name']}_{config['database']['name']}'
            if not config['database']['name'] in server.cluster[cluster]['databases']:
                newEndpointOrDatabase = True
                #JobIfy - create as job so config
                data = {
                    'name': endpointDatabase,
                    'cluster': clusterName,
                    'endpoint': config['name']
                }
                post_request_tables(clusterName, 'databases', 'insert', data)
                update_databases(cluster, config['name'], False)
                # Add Job to sync tables in cluster within this db, if any in cluster

            # if tables not exist, add
            for table in config['tables']:
                for tableName, tableConfig in table:
                    if not tableName in server.cluster[cluster]['tables']:
                        #JobIfy - create as job so config
                        data = {
                            'name': tableName,
                            'database': config['database']['name'],
                            'cluster': clusterName,
                            'config': tableConfig,
                            'isPaused': False
                        }
                        post_request_tables(clusterName, 'tables', 'insert', data)
                        update_tables(cluster, config['name'], config['database'])
                        jobsToRun.append({
                            'job': 'sync_table',
                            'cluster': clusterName,
                            'table': tableName
                            })
            # If new endpoint was added - update endpoints in each table 
            # so tables can be created in each endpoint for new / exsting tables
            if newEndpointOrDatabase == True:
                jobsToRun = [] # Resetting as all cluster tables need a job to sync on newEndpointOrDatabase 
                for table in server.cluster[cluster]['tables']:
                    for endpoint in server.cluster[cluster]['endpoints']:
                        tableEndpoint = f'{endpoint}_{table}'
                        if not tableEndpoint in server.cluster[cluster]['state']:
                            # check if this table was added along with endpoint, and does not need to be created 
                            loadState = 'loaded' if endpoint == config['name'] else 'new'
                            syncState = True if endpoint == config['name'] else False
                            # Get DB UUID for state table
                            for database in server.cluster[cluster]['databases']:
                                db = server.cluster[cluster]['databases']
                                if endpoint in db:
                                    uuid = db['uuid']

                            data = {
                                'name': tableEndpoint,
                                'state': loadState,
                                'inSync': syncState,
                                'uuid': uuid, # used for syncing logs 
                                'lastModTime': 0.0
                            }
                            post_request_tables(clusterName, 'state', 'insert', data)
                            update_state(cluster, config['name'], config['database'])
                    # Add sync_table job for each table in cluster
                    jobsToRun.append({
                        'job': 'sync_table',
                        'jobType': 'cluster',
                        'cluster': clusterName,
                        'table': table
                        })
            # Trigger in-memory refresh from DB's on each cluster node.
            post_update_cluster_config(cluster, 'update', 'cluster')

            # Create Jobs to SYNCing table
            #TODO add as global jobs table, so work can be split up among other nodes
            for job in jobsToRun:
                server.jobs.append(job)
            return server.cluster[cluster], 200

    def post_cluster_tables_config_sync(cluster, table=None):
        """
            checks for 'new' state endpoints in each cluster table and creates table in endpoint database
        """
        def table_config_sync(table):
            tb = server.cluster[cluster]['tables'][table]
            for database in server.cluster[cluster]['databases']:
                endpoint = server.cluster[cluster]['databases'][database]['endpoint']
                endpointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
                if tb['endpoinsts'][endpoint]['state'] == 'new':
                    r = requests.post(
                        '{endpointPath}/db/{database}/table/create',
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
        confg = request.get_json()
        if 'table' in config:
            post_cluster_tables_config_sync(cluster, config['table'])
        else:
            post_cluster_tables_config_sync(cluster)

    def cluster_job_manager(cluster, action, job=None):
        if action == 'add' and if not job == None:
            jobToRun = job
            jobToRun['cluster'] = cluster if not 'cluster' in job else job['cluster']
            for endpoint in server.cluster[cluster]['endpoints']:
                endpointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
                r = requests.post(
                    f'{endpoint}/cluster/jobs',
                    method='POST',
                    data = {
                        uuid.uuid1(): jobToRun
                        }
                    )
    def post_cluster_job_update_status(cluster, uuid, status):
        for endpoint in server.cluster[cluster]['endpoints']:
            endpointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
            r = requests.post(
                f'{endpoint}/cluster/job/{uuid}/{status}',
                method='POST'
            )
        
    @server.route('/cluster/job')
    def cluster_jobs(uuid):
        """
            Used by workers to pull a job from cluster job queue
        """
        if len(server.clusterJobs['jobs']) > 0:
            uuid = server.clusterJobs['jobs'].pop(0)
            post_cluster_job_update_status(
                server.clusterJobs[uuid]['cluster'], 
                uuid,
                'running'
                )
            return server.clusterJobs[uuid], 200
        else:
            return {"message": f"no jobs to process at this time"}, 200

    @server.route('/cluster/job/<uuid>/<status>', methods='POST')
    def cluster_job_update(job, status):
        if status == 'running':
            if uuid in server.clusterJobs['jobs']:
                index = server.clusterJobs['jobs'].index(uuid)
                server.clusterJobs['jobs'].pop(index)
            if uuid in server.clusterJobs:
                server.clusterJobs['status'] = status

    
    @server.route('/cluster/jobs', methods=['POST'])
    def cluster_jobs():
        """
            used for adding jobs to clusterJobs queue.
        """
        job = request.get_json()
        for uuid in job:
            server.clusterJobs[uuid]=job[uuid]
            server.clusterJobs['jobs'].append(uuid)
    @server.route('/cluster/jobs/add', methods=['POST'])
    def cluster_jobs_add():
        """
            meant to be used by node workers which will load jobs into cluster job queue
            to avoiding delays from locking during change operations
            For Example:
            # Load a job into node job queue
            server.jobs.append({'job': 'job-name', ...})
        """
        job = request.get_json()
        if 'cluster' in job:
            cluster_job_manager(job['cluster'],'add', job)
            return {"message": f"job {job} added to cluster queue"}
        else:
            return {"message": f"job {job} missing 'cluster' key required for running into cluster queue"}
    
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
            r = request.post(f'{endpoint['path']}/clusters/cron/job/{cron}/update', data=config)
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
                r = request.post(f'{endpoint['path']}/clusters/cron/job/{cron}/update', data=config)
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
                    queuedJobs[job['name']] = f'starts in {job['interval'] - queuedTime} seconds'
        return {"message": f"no jobs to start", 'queuedJobs': queuedJobs}, 200

