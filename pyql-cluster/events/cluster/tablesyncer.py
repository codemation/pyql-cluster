import sys, datetime, time, requests, json, os, random
import logging

logging.basicConfig()
    
def log(log):
    log = f" {os.environ['HOSTNAME']} - tablesyncer - {log}"
    logging.warning(log)
    return log


#clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'

nodeIP = os.environ.get('PYQL_NODE')

if os.environ.get('PYQL_TYPE') in ['K8S', 'DOCKER']:
    import socket
    nodeIP = socket.gethostbyname(socket.getfqdn())

clusterSvcName = f'http://{nodeIP}:{os.environ["PYQL_PORT"]}'
session = requests.Session()


def set_db_env(path):
    sys.path.append(path)
    import pydb
    database = pydb.get_db()
    global env
    env = database.tables['env']
    global nodeId
    nodeId = env['PYQL_ENDPOINT']
    global pyqlId
    pyqlId = env['PYQL_UUID']


def probe(path, method='GET', data=None, timeout=300.0, auth=None, **kw):
    path = f'{path}'
    if not 'token' in kw:
        auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
        token = env[auth]
    else:
        token = kw['token']
    headers = {
        'Accept': 'application/json', "Content-Type": "application/json",
        "Authentication": f"Token {token}"}
    if method == 'GET':
        r = requests.get(path, headers=headers,
                timeout=timeout)
    else:
        r = requests.post(path, headers=headers,
                data=json.dumps(data) if not data == None else data, 
                timeout=timeout)
    try:
        return r.json(),r.status_code
    except Exception as e:
        return r.text, r.status_code

def set_job_status(jobId, status, **kwargs):
    # Using - /cluster/<jobtype>/<uuid>/<status>
    return probe(
        f"{clusterSvcName}/cluster/job/syncjob/{jobId}/{status}",
        'POST',
        kwargs, 
        auth='cluster'
        )
def table_sync_recovery(cluster, table):
    """
        run when all table-endpoints are inSync=False
    """
    return probe(
        f"{clusterSvcName}/cluster/{cluster}/table/{table}/recovery", 
        'POST',
        None,
        auth='cluster'
    )
def table_copy(cluster, table, inSyncPath, inSyncToken, outOfSyncPath, outOfSyncToken):
    tableCopy, rc = probe(f'{inSyncPath}/select', token=inSyncToken)
    if rc == 500:
        error = f"#CRITICAL - tablesyncer was not able to find an inSync endpoints"
        return log(error), rc
    #/db/<database>/table/<table>/sync
    print(outOfSyncPath)
    response, rc = probe(f'{outOfSyncPath}/sync', 'POST', tableCopy, token=outOfSyncToken)
    if rc == 400:
        if 'not found in database' in response['message']:
            # create table & retry resync
            tableConfig, rc = probe(f'{inSyncPath}', token=inSyncToken)
            response, rc = probe(f'{outOfSyncPath}/create', 'POST', tableConfig, token=outOfSyncToken)
            if not rc == 200:
                failure = f"failed to create table using {tableConfig}"
                log(failure)
                return failure, 500
            #Retry sync since new table creation
            response, rc = probe(f'{outOfSyncPath}/sync', 'POST', tableCopy, token=outOfSyncToken)

    log(f"#SYNC table_copy results {response} {rc}")
    log(f"#SYNC initial table copy of {table} in cluster {cluster} completed, need to sync changes now")
    return response, rc
def table_cutover(cluster, table, action):
    """
        pause / resume table operations 
    """
    # /cluster/<cluster>/tableconf/<table>/<conf>/<action>
    return probe(f'{clusterSvcName}/cluster/{cluster}/tableconf/{table}/pause/{action}', 'POST')
def get_table_endpoints(cluster, table):
    return probe(f"{clusterSvcName}/cluster/{cluster}/table/{table}/endpoints")

def set_table_state(cluster, table, state):
    """
        Expected: {endpoint: {'state': 'new|loaded'}}
    """
    probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/set', 'POST', state)
def check_for_table_logs(cluster, table, uuid):
    #/cluster/<cluster>/tablelogs/<table>/<endpoint>/count
    return probe(
        f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/count')
    
def sync_cluster_table_logs(cluster, table, uuid, endpointPath, token):
    tryCount = 0
    while True:
        try:
            logsToSync, rc = probe(
                f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/getAll',
                auth='cluster'
            )
            break
        except Exception as e:
            log_exception(job, table, e)
            tryCount+=1
            if tryCount <= 2:
                log(f"Encountered exception trying to to pull tablelogs, retry # {tryCount}")
                continue
            break
    if not rc == 200:
        log(f"#SYNC sync_cluster_table_logs - error when pulling logs - {logsToSync} {rc}")
        return
    commitedLogs = []
    log(f"#SYNC sync_cluster_table_logs - logs to process {logsToSync}")
    txns = sorted(logsToSync['data'], key=lambda txn: txn['timestamp'])
    for txn in txns:
        if not uuid == txn['endpoint']:
            log(f"#SYNC this should not have happened, pulled logs for {uuid}")
        transaction = txn['txn']
        for action in transaction:
            message, rc = probe(f'{endpointPath}/{action}', 'POST', transaction[action], token=token)
            if rc == 200:
                commitedLogs.append(txn['uuid'])
            else:
                log(f"#SYNC this should not have happened, commiting logs for {uuid} {message} {rc}")
    # confirm txns are applied & remove from txns table
    #/cluster/<cluster>/tablelogs/<table>/<endpoint>/commit - POST
    if len(commitedLogs) > 0:
        commitResult, rc = probe(
            f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/commit',
            'POST',
            {'txns': commitedLogs}
        )
    message = f"#SYNC sync_cluster_table_logs completed for {cluster} {table}"
    log(message)
    return {"message": message}, 200

def sync_status(cluster, table, method='GET', data=None):
    # /cluster/<cluster>/tableconf/<table>/<conf>/<action>
    return probe(f'{clusterSvcName}/cluster/{cluster}/tableconf/{table}/sync/status', method, data)
def get_table_endpoint_state(cluster, table, endpoints=None):
    return probe(
        f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', 
        'POST', {'endpoints': endpoints if not endpoints == None else []})

def log_exception(job, table, e):
    message = f"{job} {table} - #SYNC Worker - ecountered exception when syncing tablelogs "
    log(message)
    log(f"{job} {table} - #SYNC Worker Exception: {repr(e)}")
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
    r, rc = probe(
        f'{clusterSvcName}/cluster/{cluster}/table/{table}/sync',
        method='POST',
        data={'job': job}
        )
    return r, rc
    
def get_and_run_job(path):
    job, rc = probe(path,'POST', {'node': nodeId}, auth='cluster') # TODO - Parameterize timeout
    if not 'config' in job or not job['config']['jobType'] == 'tablesync':
        log(f'did not find config or type when pulling job - {job}')
        return job, rc
    # Jobs pulled ['data']
    log(f"#SYNC - preparing to run {job}")
    if job['config']['jobType'] == 'tablesync':
        table = job['config']['table']
        try:
            set_job_status(job['id'],'running', message=f'tablesyncer - #SYNC starting sync_table_job for table {table}')
            result, rc = sync_table_job(job['config']['cluster'], job['config']['table'],job['id'])
        except Exception as e:
            error = f"encountered exception syncing job {job['id']} re-queueing"
            logging.exception(error)
            set_job_status(job['id'],'queued', node=None, error=error)
            return error, 500
        log(f"#SYNC get_and_run_job result {result} {rc}")
        if not rc == 200:
            warning = log(f"job {job['id']} completed with non-200 rc, requeuing")
            set_job_status(job['id'],'queued', node=None, warning=warning)
            return "job-requeued", 500
        set_job_status(job['id'],'finished')
        if 'nextJob' in job['config']:
            set_job_status(job['config']['nextJob'],'queued')        
        return log(f'tablesyncer - #SYNC completed job {job}'), 200
    log(f"#SYNC tablesyncer get_and_run_job - no job in {job}")
    return log(f"no job in {job}"), 500
if __name__=='__main__':
    args = sys.argv
    if len(args) > 1:
        jobpath, delay  = args[1], float(args[2])
        set_db_env(args[-1])
        start = time.time() - 5
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                try:
                    result, rc = probe(f'{clusterSvcName}{jobpath}')
                    #result, rc = get_and_run_job(f'{clusterSvcName}{jobpath}')
                except Exception as e:
                    logging.exception(log(f"exception when pulling / running job"))
                start = time.time()