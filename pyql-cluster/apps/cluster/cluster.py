"""
App for handling pyql-endpoint cluster requests

"""
def run(server):
    from flask import request
    import requests
    from datetime import datetime
    from random import randrange
    import json


    def update_tables(cluster, endpoint, database):
        tables = db.tables['tables'].select('*', where={
                        'cluster': cluster, 
                        'database': database['name']
                        )
        for table in tables:
            server.cluster[cluster]['tables'][table['name']] = table
            tb = server.cluster[cluster]['tables'][table['name']]
            if not 'endpoints' in tb:
                tb['endpoints'] = {}
            tb['isPaused'] == False # Used for cutover tracking & resyncs 
            tb['endpoints'][endpoint] = {
                'inSync': False,
                'path': f'{endpoint['path']}/db/{database['name']}/table/{tb['name']}'
            }


    def update_databases(cluster, endpoint):
        databases = db.tables['databases'].select('*', where={'cluster': cluster, 'endpoint': endpoint['name']})
        for database in databases:
            server.cluster[cluster]['databases'][database['name']] = database
            update_tables(cluster, endpoint, database)

    def update_endpoints(cluster):
        endpoints = db.tables['endpoints'].select('*', where={'cluster': cluster})
        for endpoint in endpoints:
            server.cluster[cluster]['endpoints'][endpoint['name']] = endpoint
            update_databases(cluster, endpoint)

    def load_clusters():
        server.cluster= dict()
        db = server.data['cluster']
        clusters = db.tables['clusters'].select('*')
        for cluster in clusters:
            update_endpoints(cluster)

    def get_endpoint_list(cluster):
        endpointList = [endpoint for endpoint in server.cluster[cluster]['endpoints']]
        return endpointList
        #return server.data['cluster'].tables['endpoints'].select('path', where={
        #    'cluster': cluster
        #})
    def get_table_endpoints(cluster, table):
        tableEndpoints = {'inSync': {}, 'outOfSync': {}}
        for endpoint in server.cluster[cluster]['endpoints']:
            if server.cluster[cluster]['endpoints'][endpoint]['inSync'] == True:
                tableEndpoints['inSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
            else:
                tableEndpoints['outOfSync'][endpoint] = server.cluster[cluster]['endpoints'][endpoint]
        return tableEndpoints

    def get_db_name(cluster, endpoint):
        return server.data['cluster'].tables['tables'].select('database', where={
                'endpoint': endpoint,
                'cluster': cluster
            })
    def get_endpoint_url(endpoint, db, table, action):
        endPointPath = server.cluster[cluster]['endpoints'][endpoint]['path']
        return f'{endPointPath}/{db}/table/{table}/{action}'

    def post_write_to_change_logs(cluster, failedEndpoint, table, data=None):
        tableEndpoints = get_table_endpoints('pyql', 'tables')
        data = request.get_json() if data == None else data
        for endpoint in tableEndpoints['inSync']:
            endPointPath = endpoint['path']
            r = request.post(
                f'{endPointPath}/cluster/{cluster}/table/{table}/endpoint/{failedEndpoint}/logs',
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                data=data
            )

    def post_update_table_status(cluster, table, status, data=None):
        endPointList = get_endpoint_list('pyql')
        tableEndpoints = get_table_endpoints('pyql', 'tables')
        data = request.get_json() if data == None else data
        for endpoint in tableEndpoints['inSync']:
            endPointPath = endpoint['path']
            r = request.post(
                f'{endPointPath}/cluster/{cluster}/table/{table}/{status}',
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
                r = requests.post(
                    get_endpoint_url(endpoint, db, table, action),
                    headers={'Accept': 'application/json', "Content-Type": "application/json"},
                    data=data,
                    timeout=0.5) 
                if not r.status_code == 200:
                    failTrack.append(endpoint)
                    # start job to Retry for endpoint, or mark endpoint bad after testing
                    print(f"unable to {action} from endpoint {endpoint}")
                else:
                    response, rc = r.json(), r.status_code
            # At least 1 success in endpoint db change, need to mark failed endpoints out of sync
            # and create a changelog for resync
            if len(failTrack) > 0 and len(failTrack) < len(tableEndpoints['inSync']):
                for failedEndpoint in failTrack:
                    endPointPath = server.cluster[cluster]['endpoints'][endpoint]['path']

                    # update sync status for table endpoint as outOfSync
                    statusData = {endpoint: {'inSync': False}}
                    post_update_table_status(cluster, table,'syncstatus' statusData)

                    # Write data to a change log for resyncing
                    post_write_to_change_logs(cluster, failedEndpoint, table, {action: data})
            # All endpoints failed request - 
            elif len(failTrack) == len(tableEndpoints['inSync']):
                return response, rc if rc is not None else r.status_code
            else:
                # No InSync failures
                pass
            # Update any previous out of sync table change-logs, if any
            for outOfSyncEndpoint in tableEndpoints['outOfSync']:
                post_write_to_change_logs(cluster, outOfSyncEndpoint, table, {action: data})

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
                return {"message": "table is paused preventing changes, maybe an issue occured during sync cutover"}, 500


    @server.route('/cluster/<cluster>/table/<table>/select', methods=['GET','POST'])
    def cluster_table_select(cluster, table):
        if request.method == 'GET':
            endPointList = get_endpoint_list(cluster)
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

    @server.route('/cluster/<cluster>/table/<table>/<conf>', methods=['GET','POST'])
    def cluster_table_status(cluster, table, conf):
        if request.method == 'POST':
            tb = server.cluster[cluster]['tables'][table]
            if conf == 'syncstatus':
                # Expected  input {endpoint: {'inSync': False}}
                config = request.get_json()
                for endpoint in config:
                    if config[endpoint]['inSync'] == False:
                        # Create Job for resyncing table endpoint
                    tb['endpoints'][endpoint]['inSync'] = config[endpoint]['inSync']
            elif conf == 'pausestatus':
                # Expected  input {'isPaused': False|True}}
                # Used for cutover period for syncing / resyncing a table in cluster
                config = request.get_json()
                tb['isPaused'] = config['isPaused']
            else:
                return {"message": f'{conf} is not a valid conf for tables'}, 400
            return {"message": f"set {config} for {table}"}
        else:
            return post_request_tables(cluster, table, 'status')
    @server.route(f'/cluster/<cluster>/tablelogs/<table>', methods=['POST'])
    def cluster_table_endpoint_logs(cluster, table):
        if request.method == 'POST':
            """ EXAMPLE txn
                {
                    'endpoint': <name>, 
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
        with open(f'{cluster}_{table}_{newLog['endpoint']}_tx.logs', 'r') as changeLogs:
            for ind, line in enumerate(changeLogs):
                if ind >= logRequest['startFromIndex'] and ind <= offset:
                    txns.append(json.loads(line))
        return {
            'totalTxns': len([i for i,v in enumerate(changeLogs)]),
            'requested': logRequest['count'],
            'txns': txns
            }




    @server.route('/cluster/<clusterName>/join', methods=['GET','POST'])
    def join_cluster(clusterName):
        required = {
            "name": "endpoint-name",
            "path": "path-to-endpoint",
            "database": "database-name",
            "tables": [
                "tb1",
                "tb2",
                "tb3"
            ]
        }
        if request.method=='GET':
            return required, 200
        else:
            print(f"join cluster for {clusterName}")
            config = request.get_json()
            db = server.data['cluster']

            #check if cluster exists
            clusterCheck = db.tables['clusters'].select('*', where={
                'name': clusterName
                })
            #if cluster does not exist, add
            if len(clusterCheck) == 0:
                #Cluster does not exist, need to create
                #JobIfy - create as job so config is replicated
                data = {
                    'name': clusterName, 
                    'createdByEndpoint': config['name'],
                    'createDate': f'{datetime.now().date()}'
                    }
                post_request_tables(clusterName, 'clusters', 'insert', data)
            #check for existing endpoint in cluster: clusterName
            endpointCheck = db.tables['endpoints'].select('*', where={
                'name': config['name'],
                'cluster': clusterName 
                })

            #if endpoint does not exist, add
            if len(endpointCheck) == 0:
                #JobIfy - create as job so config
                data = {
                    'name': config['name'],
                    'path': config['path'],
                    'cluster': clusterName
                }
                post_request_tables(clusterName, 'endpoints', 'insert', data)
                # Add Job to sync new endpoint with others, if any in cluster

            # check for existing db's in cluster: clusterName
            databaseCheck = db.tables['databases'].select('*', where={
                'name': config['database'],
                'endpoint': config['name'],
                'cluster': clusterName
            })

            # if db not exist, add
            if len(databaseCheck) == 0:
                #JobIfy - create as job so config
                data = {
                    'name': config['database'],
                    'cluster': clusterName,
                    'endpoint': config['name']
                }
                post_request_tables(clusterName, 'databases', 'insert', data)
                # Add Job to sync tables in cluster within this db, if any in cluster

            # if tables not exist, add
            for table in config['tables']:
                tableCheck = db.tables['tables'].select('*', where={
                    'name': table,
                    'database': config['database'],
                    'cluster': clusterName
                })
                if len(tableCheck) == 0:
                    #JobIfy - create as job so config
                    data = {
                        'name': table,
                        'database': config['database'],
                        'cluster': clusterName,
                        'config': 'NOT_UPDATED'
                    }
                    post_request_tables(clusterName, 'tables', 'insert', data)
                    # Create job to populate config in tables
                    clusterPath = db.tables['endpoints'].select('path', where={'cluster': 'pyql'})
                    job = {
                        'type': 'job',
                        'jobRequires': {
                            'config': {
                                'type': 'job',
                                'method': 'GET',
                                'path': f'{config['path']}/db/{config['database']}/table/{table}'
                            }
                        }
                        'method': 'POST',
                        'path': f'{clusterPath}/cluster/pyql/table/{table}/update'
                    }
                    # Add job to sync new table with other db endpoints within cluster, if any
                    server.jobs.append(job)

            return {clusterName: config}, 200

    @server.route('/cluster/<clusterName>/sync', methods=['GET','POST'])
    def cluster_config_sync(clusterName):
        # ALL DB's added to a cluster will attempt to mirror added tables to each other
        # Names of DB's will be unique within a cluster
        # DB's may exist in more than 1 cluster, but tables added in 1 cluster-db, should not exist in other clusters.
        # DB tables with same name as other DB's and different data, should be added to a different cluster.
        db = server.data['cluster']
        tablesInCluster = db.tables['tables'].select('*', where={
            'cluster': clusterName
            })
        databasesInCluster = db.tables['databases'].select('*', where={
            'cluster': clusterName
            })
        for table in tablesInCluster:
            for database in databasesInCluster:
                endpoint = db.tables['endpoints'].select('path', where{
                    cluster=clusterName,
                    name=database['endpoint']
                })
                # Starting a table create job for each table in cluster on each DB in Cluster
                job = {
                    'type': 'job',
                    'jobRequires': {
                        'config': {
                            'type': 'json',
                            'json': table['config']
                            }
                        },
                    'method': 'POST',
                    'path': f'{endpoint[0]}/db/{database}/table/create'
                    }
    
    
