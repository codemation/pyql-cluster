import sys, time, requests, json, os

clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'

def probe(path, method='GET', data=None):
    url = f'{path}'
    if method == 'GET':
        r = requests.get(url, headers={'Accept': 'application/json'})
    else:
        r = requests.post(url, headers={'Accept': 'application/json', "Content-Type": "application/json"}, data=json.dumps(data))
    try:
        return r.json(),r.status_code
    except:
        return r.text, r.status_code
def set_job_status(jobId, status, **kwargs):
    # Using - /cluster/<jobtype>/<uuid>/<status>
    return probe(
        f"{clusterSvcName}/cluster/syncjob/{jobId}/{status}",
        'POST',
        kwargs
        )

def table_select(path):
    tableSelect, rc = probe(f'{path}')
    if rc == 200:
        return tableSelect
def table_copy(cluster, table, endpointPath):
    sourcePath = f'{clusterSvcName}/cluster/{cluster}/table/{table}/select'
    tableCopy = table_select(sourcePath)
    #/db/<database>/table/<table>/sync
    print(endpointPath)
    response, rc = probe(f'{endpointPath}/sync', method='POST', data=tableCopy)
    print(f"tablesyncer - #SYNC table_copy results {response} {rc}")
    print(f"tablesyncer - #SYNC initial table copy of {table} in cluster {cluster} completed, need to sync changes now")
    return response, rc
def table_sync(cluster, table, endpointPath):
    """
        used by tables which are "loaded" to compare existing data & 
    """
def table_cutover(cluster, table, action):
    """
        pause / resume table operations 
    """
    # /cluster/<cluster>/tableconf/<table>/<conf>/<action>
    return probe(f'{clusterSvcName}/cluster/{cluster}/tableconf/{table}/pause/{action}', 'POST')
def get_table_endpoints(cluster, table):
    #/cluster/<cluster>/table/<table>/endpoints
    return probe(f"{clusterSvcName}/cluster/{cluster}/table/{table}/path")

def set_table_state(cluster, table, state):
    """
        Expected: {endpoint: {'state': 'new|loaded'}}
    """
    probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/set', 'POST', state)
def check_for_table_logs(cluster, table, uuid):
    #/cluster/<cluster>/tablelogs/<table>/<endpoint>/count
    return probe(
        f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/count')
    
def sync_cluster_table_logs(cluster, table, uuid, endpointPath):
    """
           ('endpoint', str), 
           ('uuid', str),
           ('table', str), 
           ('cluster', str),
           ('timestamp', float),
           ('txn', str)
        Expected Response from /cluster/{cluster}/tablelogs/{table}/{uuid}/getAll
        {
            'data': [

            ]
        }
    """
    #/cluster/<cluster>/tablelogs/<table>/<endpoint>/count
    tryCount = 0
    while True:
        try:
            logsToSync, rc = probe(
                f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/getAll',
            )
            break
        except Exception as e:
            tryCount+=1
            if tryCount <= 2:
                print(f"Encountered exception trying to to pull tablelogs, retry # {tryCount}")
                continue
            break
    if not rc == 200:
        print(f"tablesyncer - #SYNC sync_cluster_table_logs - error when pulling logs - {logsToSync} {rc}")
        return
    commitedLogs = []
    print(f"tablesyncer - #SYNC sync_cluster_table_logs - logs to process {logsToSync}")
    for txn in logsToSync['data']:
        if not uuid == txn['endpoint']:
            print(f"tablesyncer - #SYNC this should not have happened, pulled logs for {uuid}")
        transaction = txn['txn']
        for action in transaction:
            message, rc = probe(f'{endpointPath}/{action}', 'POST', transaction[action])
            if rc == 200:
                commitedLogs.append(txn['uuid'])
            else:
                print(f"tablesyncer - #SYNCthis should not have happened, commiting logs for {uuid} {message} {rc}")
    # confirm txns are applied & remove from txns table
    #/cluster/<cluster>/tablelogs/<table>/<endpoint>/commit - POST
    if len(commitedLogs) > 0:
        commitResult, rc = probe(
            f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/commit',
            'POST',
            {'txns': commitedLogs}
        )
    message = f"tablesyncer - #SYNC sync_cluster_table_logs completed for {cluster} {table}"
    print(message)
    return {"message": message}, 200

def sync_cluster_table(cluster, table):
    """
        checks for 'new' state endpoints in each cluster table and creates table in endpoint database
    """
    return probe(f'{clusterSvcName}/cluster/{cluster}/sync', 'POST', {'table': table})

def sync_status(cluster, table, method='GET', data=None):
    # /cluster/<cluster>/tableconf/<table>/<conf>/<action>
    return probe(f'{clusterSvcName}/cluster/{cluster}/tableconf/{table}/sync/status', method, data)
def get_table_endpoint_state(cluster, table, endpoints=[]):
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', 'POST', {'endpoints': endpoints})
def create_table(table):
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', 'POST', {'endpoints': endpoints})
def drop_table(table):
    pass

def get_job_requirement(job):
    #TODO - convert table config from json string to dict via json.loads
    pass
def log_exception(job, table, e):
    message = f"tablesyncer {job} {table} - #SYNC Worker - ecountered exception when syncing tablelogs "
    print(message)
    print(f"tablesyncer {job} {table} - #SYNC Worker Exception: {repr(r)}")
    return message

def sync_table_job(cluster, table, job=None):
    """
        - Job is started for syncing TB in new DB
        - Worker claims sync job & issues a startSync for TB, this starts log generation for new changes
        - Worker completes initial insertions from select * of TB.
        - Worker starts to pull changes from change logs 
        - Worker completes pull of change logs & issues a cutover by pausing table.
        - Worker checks for any new change logs that were commited just before the table was paused, and also syncs these.
        - Worker sets new TB endpoint as inSync=True & unpauses TB
        - SYNC job is completed
    """
    # check sync status by GET to /cluster/<cluster>/table/<table>/sync/status
    #syncStatus, rc = sync_status(cluster, table)
    syncStatus, rc = get_table_endpoints(cluster, table)

    if not rc == 200:
        return {"message": f"error checking sync/status of {cluster} {table} {syncStatus}"}, rc
    """
    Value returned from syncStatus
            {
        "inSync": {
            "dev-server02": {
            "cluster": "pyql",
            "name": "dev-server02",
            "path": "192.168.3.33:8080"
            }
        },
        "outOfSync": {
            "dev-server01": {
            "cluster": "pyql",
            "name": "dev-server01",
            "path": "192.168.122.100"
            }
        }
        }
    """
    endpointsToSync = []
    print(syncStatus)
    for endpoint in syncStatus['outOfSync']:
        endpointsToSync.append((endpoint, syncStatus['outOfSync'][endpoint]))
    if len(endpointsToSync) == 0:
        return {"message": f"no endpoints to sync in cluster {cluster} table {table}"}, 200

    # check current state of table endpoints
    stateCheck, rc = get_table_endpoint_state(cluster, table, [ep[0] for ep in endpointsToSync])

    # Sync Cluster Table Config
    sync_cluster_table(cluster,table)
    print(f"tablesyncer {job} {table} - #SYNC stateCheck {stateCheck}")
    
    for endpoint in endpointsToSync:
        uuid = stateCheck[endpoint[0]]['uuid']
        # Check if table is fresh 
        def load_table():
            endpointPath = endpoint[1]
            # Issue POST to /cluster/<cluster>/table/<table>/state/set -> loaded 
            state = {endpoint[0]: {'state': 'loaded'}}
            set_table_state(cluster, table, state)
            # Worker completes initial insertions from select * of TB.
            table_copy(cluster, table, endpointPath)
        try:
            if stateCheck[endpoint[0]]['state'] == 'new': # Never loaded, needs to be initialize
                print(f"tablesyncer {job} {table} - #SYNC table {table} never loaded, needs to be initialize")
                load_table()
            else:
                # Check for un-commited logs - otherwise full resync needs to occur.
                print(f"tablesyncer {job} {table} - #SYNC table {table} never loaded, needs to be initialize")
                count, rc = check_for_table_logs(cluster, table, uuid)
                if rc == 200:
                    if count['availableTxns'] > 0:
                        pass
                    else:
                        print(f"tablesyncer {job} {table} - #SYNC Need to reload table {table} - drop / load")
                        # Need to reload table - drop / load
                        load_table()
                        
            print(f"tablesyncer {job} {table} - #SYNC Worker starts to pull changes from change logs")
            
            outOfSyncPath = stateCheck[endpoint[0]]['path']
            sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
        except Exception as e:
            return log_exception(job, table, e), 500

            
        print(f"tablesyncer {job} {table} - #SYNC Worker completes pull of change logs & issues a cutover by pausing table.")
        message, rc = table_cutover(cluster, table, 'start')
        try:
            print(f"tablesyncer {job} {table} - #SYNC Worker checks for any new change logs that were commited just before the table was paused, and also syncs these")
            sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
            print(f"tablesyncer {job} {table} - #SYNC Worker sets new TB endpoint as inSync=True")
            setInSync = {endpoint[0]: {'inSync': True}}
            statusResult, rc = sync_status(cluster, table, 'POST', setInSync)
            print(f"tablesyncer {job} {table} - #SYNC set {table} {setInSync} result: {statusResult} {rc}")
            print(f"tablesyncer {job} {table} - #SYNC marking outOfSync endpoint {endpoint[0]} table {table} in {cluster} as {setInSync}")
            #if table == 'state':
            #    sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
            #setInSync = {endpoint[0]: {'inSync': True}}
            #statusResult, rc = sync_status(cluster, table, 'POST', setInSync)
        except Exception as e:
            print(f"tablesyncer {job} {table} - #SYNC Worker rolling back pause / inSync")
            setInSync = {endpoint[0]: {'inSync': False}}
            statusResult, rc = sync_status(cluster, table, 'POST', setInSync)
            #UnPause
            message, rc = table_cutover(cluster, table, 'stop')
            return log_exception(job, table, e), 500

        # Un-Pause
        print(f"tablesyncer {job} {table} - #SYNC Worker -  completes cutover by un-pausing table")
        message, rc = table_cutover(cluster, table, 'stop')
        message = f'tablesyncer {job} {table} - #SYNC finished syncing {endpoint[0]} for table {table} in cluster {cluster}'
        print(message)
    return {"message": message}, 200
    
def get_and_run_job(path):
    job, rc = probe(path,'POST', {'node': os.environ['PYQL_NODE']})
    if not "message" in job:
        # Jobs pulled ['data']
        print(f"tablesyncer - #SYNC - preparing to run {job}")
        if job['config']['jobType'] == 'tablesync':
            table = job['config']['table']
            set_job_status(job['id'],'running', message=f'tablesyncer - #SYNC starting sync_table_job for table {table}')
            result, rc = sync_table_job(job['config']['cluster'], job['config']['table'],job['id'])
            print(f"tablesyncer - #SYNC get_and_run_job result {result} {rc}")
            if not rc == 200:
                set_job_status(job['id'],'queued', node=None)
                return "job-requeued", 500

            set_job_status(job['id'],'finished')
            if 'nextJob' in job['config']:
                set_job_status(job['config']['nextJob'],'queued')
            #TODO Add exception handling here.
            # Using - /cluster/<jobtype>/<uuid>/<status>
            result = f'tablesyncer - #SYNC completed job {job}'
            return result, 200
        print(f"tablesyncer - #SYNC tablesyncer get_and_run_job - no job in {job}")
        return f"no job in {job}", 500
    print(f"get_and_run_job {job} rc")
    return job, rc
if __name__=='__main__':
    args = sys.argv
    if len(args) > 1:
        jobpath, delay  = args[1], float(args[2])
        start = time.time()
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                result, rc = get_and_run_job(f'{clusterSvcName}{jobpath}')
                start = time.time()