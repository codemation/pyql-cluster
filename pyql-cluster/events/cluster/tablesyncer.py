#TODO - Check if anything else makes sense to move into clusters app instead of as worker
#TODO - fix requing if job finishes with non-rc200 i.e: 
# 2020-02-11T21:19:02.462547 192-168-231-251 tablesyncer - job f55e54ec-4d13-11ea-9d4c-d6c50f362472 completed with non-200 rc, requeuing
# if node is not removed from job, cannot be run again, this can lock waiting jobs


import sys, datetime, time, requests, json, os

def log(log):
    time = datetime.datetime.now().isoformat()
    print(f"{time} {os.environ['HOSTNAME']} tablesyncer - {log}")
    


clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'

nodeIp = os.environ['PYQL_ENDPOINT']
log(f"started with endpoint {nodeIp}")

def probe(path, method='GET', data=None, timeout=3.0):
    url = f'{path}'
    try:
        if method == 'GET':
            r = requests.get(url, headers={'Accept': 'application/json'}, timeout=timeout)
        else:
            r = requests.post(url, headers={'Accept': 'application/json', "Content-Type": "application/json"}, data=json.dumps(data), timeout=timeout)
    except Exception as e:
        error = f"Encountered exception when probing {path} - {repr(e)}"
        return error, 500
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
def table_sync_recovery(cluster, table):
    """
        run when all table-endpoints are inSync=False
    """
    return probe(
        f"{clusterSvcName}/cluster/{cluster}/table/{table}/recovery", 
        'POST',
    )
def table_copy(cluster, table, endpointPath):
    sourcePath = f'{clusterSvcName}/cluster/{cluster}/table/{table}/select'
    tableCopy, rc = probe(sourcePath)
    if rc == 500:
        error = f"#CRITICAL - tablesyncer was not able to find an inSync endpoints"
        log(error)
        return error, rc
    #/db/<database>/table/<table>/sync
    print(endpointPath)
    response, rc = probe(f'{endpointPath}/sync', 'POST', tableCopy)
    if rc == 400:
        if 'not found in database' in response['message']:
            # create table & retry resync
            tableConfig, rc = probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}')
            response, rc = probe(f'{endpointPath}/create', 'POST', tableConfig)
            if not rc == 200:
                failure = f"failed to create table using {tableConfig}"
                log(failure)
                return failure, 500
            #Retry sync since new table creation
            response, rc = probe(f'{endpointPath}/sync', 'POST', tableCopy)

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
    tryCount = 0
    while True:
        try:
            logsToSync, rc = probe(
                f'{clusterSvcName}/cluster/{cluster}/tablelogs/{table}/{uuid}/getAll',
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
    for txn in logsToSync['data']:
        if not uuid == txn['endpoint']:
            log(f"#SYNC this should not have happened, pulled logs for {uuid}")
        transaction = txn['txn']
        for action in transaction:
            message, rc = probe(f'{endpointPath}/{action}', 'POST', transaction[action])
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
def get_table_endpoint_state(cluster, table, endpoints=[]):
    return probe(f'{clusterSvcName}/cluster/{cluster}/table/{table}/state/get', 'POST', {'endpoints': endpoints})

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
    # check sync status by GET to /cluster/<cluster>/table/<table>/sync/status
    syncStatus, rc = get_table_endpoints(cluster, table)

    if not rc == 200:
        return {"message": f"error checking sync/status of {cluster} {table} {syncStatus}"}, rc
    endpointsToSync = []
    log(f"syncStatus {syncStatus}")
    for endpoint in syncStatus['outOfSync']:
        endpointsToSync.append((endpoint, syncStatus['outOfSync'][endpoint]))
    if len(endpointsToSync) == 0:
        return {"message": f"no endpoints to sync in cluster {cluster} table {table}"}, 200

    # check current state of table endpoints
    stateCheck, rc = get_table_endpoint_state(cluster, table, [ep[0] for ep in endpointsToSync])

    log(f"{job} {table} - #SYNC stateCheck {stateCheck}")
    
    for endpoint in endpointsToSync:
        uuid = stateCheck[endpoint[0]]['uuid']
        endpointPath = stateCheck[endpoint[0]]['path']
        # check if endpoint uuid is alive & inQuorum
        if cluster == 'pyql':
            quorum, rc = probe(f'{clusterSvcName}/pyql/quorum')
            if not uuid in quorum['quorum']['nodes']['nodes']:
                warning = f"endpoint {uuid}  is not alive or inQuorum with cluster - cannot issue sync right now"
                return log_exception(job, table, warning), 500
        else:
            dbAlivecheck, rc = probe(f"{endpointPath}")
            if not rc == 200 and not rc==404:
                log(f"dbAlivecheck response {dbAlivecheck}")
                warning = f"endpoint {uuid}  is not alive or reachable with path {endpointPath} - cannot issue sync right now"
                return log_exception(job, table, warning), 500

        # Check if table is fresh 
        def load_table():
            # Issue POST to /cluster/<cluster>/table/<table>/state/set -> loaded 
            state = {endpoint[0]: {'state': 'loaded'}}
            set_table_state(cluster, table, state)
            # Worker completes initial insertions from select * of TB.
            #if cluster == 'pyql' and table == 'jobs' or table == 'transactions' or table=='state':
            if cluster == 'pyql' and table == 'transactions':
                #need to blackout changes to these tables during copy as txn logs not generated
                message, rc = table_cutover(cluster, table, 'start')
                response, rc = table_copy(cluster, table, endpointPath)
                if rc == 500:
                    #TODO - Handle table not yet existing yet, create table using out of f'{clusterSvcName}/cluster/{cluster}/table/{table}
                    response, rc = table_sync_recovery(cluster, table)
                    return log_exception(job, table, response), 500

                tableEndpoint = f'{endpoint[0]}{table}'
                setInSync = {tableEndpoint: {'inSync': True, 'state': 'loaded'}}
                statusResult, rc = sync_status(cluster, table, 'POST', setInSync)
                message, rc = table_cutover(cluster, table, 'stop')
                return
            response, rc = table_copy(cluster, table, endpointPath)
            if rc == 500:
                response, rc = table_sync_recovery(cluster, table)
                return log_exception(job, table, response), 500
        try:
            if stateCheck[endpoint[0]]['state'] == 'new': # Never loaded, needs to be initialize
                log(f"{job} {table} - #SYNC table {table} never loaded, needs to be initialize")
                load_table()
            else:
                # Check for un-commited logs - otherwise full resync needs to occur.
                log(f"{job} {table} - #SYNC table {table} never loaded, needs to be initialize")
                count, rc = check_for_table_logs(cluster, table, uuid)
                if rc == 200:
                    if not count['availableTxns'] > 0:
                        log(f" {job} {table} - #SYNC Need to reload table {table} - drop / load")
                        # Need to reload table - drop / load
                        load_table()
                        
            log(f"{job} {table} - #SYNC Worker starts to pull changes from change logs")
            
            outOfSyncPath = stateCheck[endpoint[0]]['path']
            sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
        except Exception as e:
            return log_exception(job, table, e), 500

        if cluster == 'pyql' and table == 'jobs' or table == 'transactions':
            pass
        else:
            log(f"{job} {table} - #SYNC Worker completes pull of change logs & issues a cutover by pausing table.")
            message, rc = table_cutover(cluster, table, 'start')
            tableEndpoint = f'{endpoint[0]}{table}'
            try:
                log(f"{job} {table} - #SYNC Worker checks for any new change logs that were commited just before the table was paused, and also syncs these")
                sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
                log(f"{job} {table} - #SYNC Worker sets new TB endpoint as inSync=True")
                
                setInSync = {tableEndpoint: {'inSync': True, 'state': 'loaded'}}
                statusResult, rc = sync_status(cluster, table, 'POST', setInSync)
                log(f"{job} {table} - #SYNC set {table} {setInSync} result: {statusResult} {rc}")
                log(f"{job} {table} - #SYNC marking outOfSync endpoint {f'{endpoint[0]}{table}'} table {table} in {cluster} as {setInSync}")
                if cluster == 'pyql' and table == 'state':
                    sync_cluster_table_logs(cluster, table, uuid, outOfSyncPath)
                    sync_status(cluster, table, 'POST', setInSync)
            except Exception as e:
                log_exception(job, table, e)
                log(f"{job} {table} - #SYNC Worker rolling back pause / inSync")
                setInSync = {tableEndpoint: {'inSync': False, 'state': 'loaded'}}
                statusResult, rc = sync_status(cluster, table, 'POST', setInSync)
                #UnPause
                message, rc = table_cutover(cluster, table, 'stop')
                return log_exception(job, table, e), 500

            # Un-Pause
            log(f"{job} {table} - #SYNC Worker -  completes cutover by un-pausing table")
            message, rc = table_cutover(cluster, table, 'stop')

        message = f'{job} {table} - #SYNC finished syncing {endpoint[0]} for table {table} in cluster {cluster}'
        log(message)
    return {"message": message}, 200
    
def get_and_run_job(path):
    job, rc = probe(path,'POST', {'node': nodeIp}, timeout=30.0) # TODO - Parameterize timeout
    if not 'config' in job or not job['config']['jobType'] == 'tablesync':
        log(job['message'])
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
            log(error)
            log(f"Exception {repr(e)}")
            set_job_status(job['id'],'queued', node=None)
            return error, 500
        log(f"#SYNC get_and_run_job result {result} {rc}")
        if not rc == 200:
            log(f"job {job['id']} completed with non-200 rc, requeuing")
            set_job_status(job['id'],'queued', node=None)
            return "job-requeued", 500
        set_job_status(job['id'],'finished')
        if 'nextJob' in job['config']:
            set_job_status(job['config']['nextJob'],'queued')
        result = f'tablesyncer - #SYNC completed job {job}'
        log(result)
        return result, 200
    log(f"#SYNC tablesyncer get_and_run_job - no job in {job}")
    return f"no job in {job}", 500
if __name__=='__main__':
    args = sys.argv
    if len(args) > 1:
        jobpath, delay  = args[1], float(args[2])
        start = time.time() - 5
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                try:
                    result, rc = get_and_run_job(f'{clusterSvcName}{jobpath}')
                except Exception as e:
                    log(f"exception when pulling / running job")
                    log(repr(e))
                start = time.time()