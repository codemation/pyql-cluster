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
def table_select(path):
    tableSelect, rc = probe(f'{path}')
    if rc == 200:
        return tableSelect
def table_copy(cluster, table, endpointPath):
    sourcePath = f'{clusterSvcName}/cluster/{cluster}/table/{table}/select'
    tableCopy = table_select(sourcePath)
    #/db/<database>/table/<table>/sync
    response, rc = probe(f'{endpointPath}/sync', method='POST', data=tableCopy)
    print(f"initial table copy of {table} in cluster {cluster} completed, need to sync changes now")
    return response, rc
def table_sync(cluster, table, endpointPath):
    """
        used by tables which are "loaded" to compare existing data & 
    """
def table_cutover(cluster, table, action):
    """
        pause / resume table operations 
    """
    probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/pause/{action}', method='POST')

def set_table_state(cluster, table, state):
    """
        Expected: {endpoint: {'state': 'new|loaded'}}
    """
    probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/set', method='POST', data=state)
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
    logsToSync, rc = probe(
        f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/getAll',
    )
    if not rc == 200:
        print(f"sync_cluster_table_logs - error when pulling logs - {logsToSync} {rc}")
        return
    commitedLogs = []
    for txn in logsToSync['data']:
        if not uuid == txn['endpoint']:
            print(f"this should not have happened, pulled logs for {uuid}")
        for action in txn['txn']:
            message, rc = probe(f'{endpointPath}/{action}', method='POST', data=txn['txn'][action])
            if rc == 200:
                commitedLogs.append(txn['uuid'])
            else:
                print(f"this should not have happened, commiting logs for {uuid} {message} {rc}")
    # confirm txns are applied & remove from txns table
    #/cluster/<cluster>/tablelogs/<table>/<endpoint>/commit - POST
    if len(commitedLogs) > 0:
        commitResult, rc = probe(
            f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/commit',
            'POST',
            {'txns': commitedLogs}
        )
    message = f"sync_cluster_table_logs completed for {cluster} {table}"
    print(message)
    return {"message": message}, 200

def sync_cluster_table(cluster, table):
    """
        checks for 'new' state endpoints in each cluster table and creates table in endpoint database
    """
    return probe(f'{clusterSvcName}/cluster/{cluster}/sync', method='POST', data={'table': table})

def sync_status(cluster, table, method='GET', data=None):
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/sync/status', method=method, data=data)
def get_table_endpoint_state(cluster, table, endpoints=[]):
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', method='POST', data={'endpoints': endpoints})
def create_table(table):
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', method='POST', data={'endpoints': endpoints})
def drop_table(table):
    pass

def get_job_requirement(job):
    #TODO - convert table config from json string to dict via json.loads
    pass

def sync_table_job(cluster, table):
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
    syncStatus, rc = sync_status(cluster, table)
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
        endpointsToSync.append((endpoint, syncStatus['outOfSync'][endpoint]['path']))

    # check current state of table endpoints
    stateCheck, rc = get_table_endpoint_state(cluster, table, [ep[0] for ep in endpointsToSync])

    # Sync Cluster Table Config
    sync_cluster_table(cluster,table)
    print(f"stateCheck {stateCheck}")
    uuid = stateCheck[endpoint[0]]['uuid']

    for endpoint in endpointsToSync:
        # Check if table is fresh 
        def load_table():
            endpointPath = endpoint[1]
            # Issue POST to /cluster/<cluster>/table/<table>/state/set -> loaded 
            state = {endpoint: {'state': 'loaded'}}
            set_table_state(cluster, table, state)
            # Worker completes initial insertions from select * of TB.
            table_copy(cluster, table, endpointPath)

        if stateCheck[endpoint[0]]['state'] == 'new': # Never loaded, needs to be initialize
            load_table()
        else:
            # Check for un-commited logs - otherwise full resync needs to occur.
            count, rc = check_for_table_logs(cluster, table, uuid)
            if rc == 200:
                if count['availableTxns'] > 0:
                    pass
                else:
                    # Need to reload table - drop / load
                    load_table()
                    
        # Worker starts to pull changes from change logs
        
        outOfSyncPath = stateCheck[endpoint[0]]['path']
        sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
        print("# Worker completes pull of change logs & issues a cutover by pausing table.")
        table_cutover(cluster, table, 'start')
        print("# Worker checks for any new change logs that were commited just before the table was paused, and also syncs these")
        sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
        print("# Worker sets new TB endpoint as inSync=True & unpauses TB")
        setInSync = {endpoint[0]: {'inSync': True}}
        statusResult, rc = sync_status(cluster, table, 'POST', data=setInSync)
        print(f"marking outOfSync endpoint {endpoint[0]} table {table} in {cluster} as {setInSync}")
        table_cutover(cluster, table, 'stop')
        print ("# SYNC job is completed for endpoint")
        print(f'finished syncing {endpoint} for table {table} in cluster {cluster}')
    
def get_and_run_job(path):
    job, rc = probe(path)
    if not "message" in job:
        # Jobs pulled
        print(f"preparing to run {job}")
        if job['job'] == 'sync_table':
            sync_table_job(job['cluster'], job['table'])
            #TODO Add exception handling here.
            result = f'completed job {job}'
            return result, 200
        return f"no job in {job}", 500
    return job, rc
if __name__=='__main__':
    args = sys.argv
    if len(args) > 1:
        jobpath, delay  = args[1], float(args[2])
        start = delay
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                result, rc = get_and_run_job(f'{clusterSvcName}{jobpath}')
                start = time.time()