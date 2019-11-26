import sys, time, requests, json

clusterSvcName = 'http://localhost:8090' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']

def probe(path, method='GET', data=None):
    """
        default staring is http://localhost:8080
    """
    url = f'{path}'
    if method == 'GET' 
        r = requests.get(url, headers={'Accept': 'application/json'})
    else:
        r = request.post(url, headers={'Accept': 'application/json', "Content-Type": "application/json"}, data=data)
    try:
        return r.json(),r.status_code
    except:
        return r.text, r.status_code
def table_select(cluster, table, path):
    tableSelect, rc = probe(f'{path}')
    if rc == 200:
        return tableSelect
def table_copy(cluster, table, endpointPath):
    sourcePath = f'{clusterSvcName}/cluster/{cluster}/table/{table}/select'
    syncPath = f'{endpointPath}/select'
    if not len(table_select(cluster, table, syncPath)) == 0:
        tableCopy = table_select(cluster, table, sourcePath)
        for row in tableCopy:
            probe(f'{endpointPath}/insert', method='POST', data=row)
        print(f"initial table copy of {table} in cluster {cluster} completed, need to sync changes now")
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
def sync_cluster_table_logs(cluster, table, uuid, endpointPath):
    """
        Expected input:
        {
            'count': 5,
            'startFromIndex': 0 
        }
        Expected Response from /cluster/{cluster}/tablelogs/{table}/{uuid}
        {
                'totalTxns': len([i for i,v in enumerate(changeLogs)]),
                'requested': logRequest['count'],
                'txns': [{
                    'endpoint': 'uuid', 
                    'txn': {
                        'update': {
                            'set': {'name': 'new'}, 
                            'where': {'key': 1}
                        }
                    }]
                }
        }
    """
    index = 0
    while True:
        logsToSync, rc = probe(
            f'/cluster/{cluster}/tablelogs/{table}/{uuid}', 
            data={'count': 5, 'startFromIndex': index})
        if rc == 200:
            for log in logsToSync['txns']:
                if 'endpoint' == uuid:
                    for action in log['txn']:
                        probe(f'{endpointPath}/{action}', method='POST', data=log['txn'][action])
            if logsToSync['totalTxns'] - len(totalTxns['txns']+index) > 0:
                index = index + len(totalTxns['txns']
                continue
            else:
                print(f'completed log replay of {table} in cluster {cluster}')
                break
        else:
            print(f'error in log replay of {table} in cluster {cluster} {rc.message} {rc.status_code}')
            break
    



def sync_cluster_table(cluster, table):
    clusterSvcName = 'http://localhost:8090' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']
    probe(f'{clusterSvcName}/cluster/{cluster}/sync', method='POST', data={'table': table})
def sync_status(cluster, table, method='GET', data=None):
    clusterSvcName = 'http://localhost:8090' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/sync/status', method=method, data=data)
def get_table_endpoint_state(cluster, table, endpoints=[])
    clusterSvcName = 'http://localhost:8090' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', method='POST', data={'endpoints': endpoints})
def create_table(table):
    clusterSvcName = 'http://localhost:8090' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', method='POST', data={'endpoints': endpoints})
def drop_table(table):
    pass

def get_job_requirement(job):
    #TODO - convert table config from json string to dict via json.loads

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
    syncStatus = sync_status(cluster, table)
    endpointsToSync = []
    for endpoint in syncStatus['endpoints']:
        if syncStatus['endpoints'][endpoint]['inSync'] == False
            endpointsToSync.append((endpoint, syncStatus['endpoints'][endpoint]['path']))
    stateCheck = get_table_endpoint_state(cluster, table, [ep[0] for ep in endpointsToSync])

    # Sync Cluster Table Config
    sync_cluster_table(cluster,table)

    for endpoint in endpointsToSync:
        # Check if table is fresh 
        if stateCheck[endpoint[0]]['state'] == 'new': # Never loaded, needs to be initialize
            endpointPath = endpoint[1]

            # Issue POST to /cluster/<cluster>/table/<table>/state/set -> loaded 
            state = {endpoint: {'state': 'loaded'}}
            set_table_state(cluster, table, state)

            # Worker completes initial insertions from select * of TB.
            table_copy(cluster, table, endpointPath)

        # Worker starts to pull changes from change logs
        uuid = stateCheck[endpoint[0]]['uuid']
        sync_cluster_table_logs(cluster, table, uuid)
        # Worker completes pull of change logs & issues a cutover by pausing table.
        table_cutover(cluster, table, 'start')
        # Worker checks for any new change logs that were commited just before the table was paused, and also syncs these
        sync_cluster_table_logs(cluster, table, uuid)
        # Worker sets new TB endpoint as inSync=True & unpauses TB
        sync_status(cluster, table, 'POST', {endpoint: {'inSync': True}})
        table_cutover(cluster, table, 'stop')
        # SYNC job is completed for endpoint
        print(f'finished syncing {endpoint} for table {table} in cluster {cluster}')
    
def get_and_run_job(path):
    job, rc = probe(path)
    if not "message" in job:
        # Jobs pulled
        print(f"preparing to run {job}")
        if job['job'] == 'sync_table':
            """ 
                Job requires
                {
                    'job': 'sync_table',
                    'cluster': clusterName,
                    'table': tableName
                }
            """
            sync_table_job(job['cluster'], job['table'])
            #TODO Add exception handling here.
            result = f'completed job {job}'
            return result, 200
        return f"no job in {job}", 500
    return job, rc
if __name__=='__main__':
    args = sys.argv
    if len(args) > 3:
        jobpath, delay  = args[1], float(args[2])
        start = delay
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                result, rc = get_and_run_job(jobpath)
                start = time.time()