#TODO - Check if anything else makes sense to move into clusters app instead of as worker
#TODO - fix requing if job finishes with non-rc200 i.e: 
# 2020-02-11T21:19:02.462547 192-168-231-251 tablesyncer - job f55e54ec-4d13-11ea-9d4c-d6c50f362472 completed with non-200 rc, requeuing
# if node is not removed from job, cannot be run again, this can lock waiting jobs


import sys, datetime, time, requests, json, os, random
import logging

logging.basicConfig()

def log(log):
    time = datetime.datetime.now().isoformat()
    print(f"{time} {os.environ['HOSTNAME']} tablesyncer - {log}")
    


clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'

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


def probe(path, method='GET', data=None, timeout=3.0, auth=None, **kw):
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
                timeout=1.0)
    else:
        r = requests.post(path, headers=headers,
                data=json.dumps(data), timeout=1.0)
    try:
        return r.json(),r.status_code
    except Exception as e:
        return r.text, r.status_code

def set_job_status(jobId, status, **kwargs):
    # Using - /cluster/<jobtype>/<uuid>/<status>
    return probe(
        f"{clusterSvcName}/cluster/pyql/syncjob/{jobId}/{status}",
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
        log(error)
        return error, rc
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
    # get table endpoints
    tableEndpoints, rc =  get_table_endpoints(cluster, table)
    if not rc == 200:
        log(f"error checking sync/status of {cluster} {table} - {tableEndpoints}")
        return {"message": f"error checking sync/status of {cluster} {table} {tableEndpoints}"}, rc
    # Sample tableEndpoints response
    # {
    # "inSync": {
    #     "b492c932-670a-11ea-9791-63f7c3e0f14f": {
    #     "cluster": "b7cae0d0-670a-11ea-9791-63f7c3e0f14f",
    #     "dbname": "cluster",
    #     "path": "192.168.3.33:8090",
    #     "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6ImI0OTJjOTM1LTY3MGEtMTFlYS05NzkxLTYzZjdjM2UwZjE0ZiIsImV4cGlyYXRpb24iOiJuZXZlciJ9.Pdw0uFdhHZGpvwF_0NkZsj0oYssINF-zuoEkDLwWHaA",
    #     "uuid": "b492c932-670a-11ea-9791-63f7c3e0f14f"
    #     }
    # },
    # "outOfSync": {}
    # }

    # check for endpoints to sync
    endpointsToSync = [endpoint for endpoint in tableEndpoints['outOfSync']]

    if len(endpointsToSync) == 0:
        return {"message": f"no endpoints to sync in cluster {cluster} table {table}"}, 200

    for endpoint in endpointsToSync:
        # outOfSync endpoint to sync
        ep = tableEndpoints['outOfSync'][endpoint]
        print(ep)
        print(f"tableEndpoints - {tableEndpoints}")
        uuid, path, token, db, tableState = ep['uuid'], ep['path'], ep['token'], ep['dbname'], ep['state']
        clusterId = ep['cluster']
        endpointPath = f"http://{path}/db/{db}/table/{table}"

        # inSync endpoint To sync against
        inSync = list(tableEndpoints['inSync'].keys())[random.randrange(len([k for k in tableEndpoints['inSync']]))]
        inSyncEndoint = tableEndpoints['inSync'][inSync] 
        inSyncPath =  f"http://{inSyncEndoint['path']}/db/{db}/table/{table}"
        inSyncToken = inSyncEndoint['token']

        # check if endpoint in alive
        r, rc = probe(f'http://{path}/pyql/node')
        if not rc == 200 and not rc==404:
            warning = f"endpoint {uuid} is not alive or reachable with path {path} - cannot issue sync right now - {r} {rc}"
            return log_exception(job, table, warning), 500

        def load_table():
            # Issue POST to /cluster/<cluster>/table/<table>/state/set -> loaded 
            state = {endpoint: {'state': 'loaded'}}
            set_table_state(clusterId, table, state)
            # Worker completes initial insertions from select * of TB.
            if clusterId == pyqlId and table == 'transactions':
                #need to blackout changes to these tables during copy as txn logs not generated
                message, rc = table_cutover(clusterId, table, 'start')
                response, rc = table_copy(clusterId, table, inSyncPath, inSyncToken, endpointPath, token)
                if rc == 500:
                    #TODO - Handle table not yet existing yet, create table using out of f'{clusterSvcName}/cluster/{cluster}/table/{table}
                    response, rc = table_sync_recovery(clusterId, table)
                    return log_exception(job, table, response), 500

                tableEndpoint = f'{endpoint}{table}'
                setInSync = {tableEndpoint: {'inSync': True, 'state': 'loaded'}}
                statusResult, rc = sync_status(clusterId, table, 'POST', setInSync)
                message, rc = table_cutover(clusterId, table, 'stop')
                return
            response, rc = table_copy(clusterId, table, inSyncPath, inSyncToken, endpointPath, token)
            if rc == 500:
                response, rc = table_sync_recovery(clusterId, table)
                return log_exception(job, table, response), 500

        if tableState == 'new':
            log(f"{job} {table} - #SYNC table {table} never loaded, needs to be initialize")
            load_table()
        else:
            # Check for un-commited logs - otherwise full resync needs to occur.
            log(f"{job} {table} - #SYNC table {table} already loaded, checking for change logs")
            count, rc = check_for_table_logs(clusterId, table, uuid)
            if rc == 200:
                if not count['availableTxns'] > 0:
                    log(f" {job} {table} - #SYNC Need to reload table {table} - drop / load")
                    # Need to reload table - drop / load
                    load_table()
        log(f"{job} {table} - #SYNC Worker starts to pull changes from change logs")
        sync_cluster_table_logs(clusterId, table, uuid, endpointPath, token)

        if clusterId == pyqlId and table == 'transactions':
            pass
        else:
            log(f"{job} {table} - #SYNC Worker completes pull of change logs & issues a cutover by pausing table.")
            message, rc = table_cutover(clusterId, table, 'start')
            tableEndpoint = f'{endpoint}{table}'
            try:
                log(f"{job} {table} - #SYNC Worker checks for any new change logs that were commited just before the table was paused, and also syncs these")
                sync_cluster_table_logs(clusterId, table, uuid, endpointPath, token)
                log(f"{job} {table} - #SYNC Worker sets new TB endpoint as inSync=True")
                setInSync = {tableEndpoint: {'inSync': True, 'state': 'loaded'}}
                statusResult, rc = sync_status(clusterId, table, 'POST', setInSync)
                log(f"{job} {table} - #SYNC set {table} {setInSync} result: {statusResult} {rc}")
                log(f"{job} {table} - #SYNC marking outOfSync endpoint {tableEndpoint} in {cluster} as {setInSync}")
                if cluster == pyqlId and table == 'state':
                    #sync_cluster_table_logs(clusterId, table, uuid, endpointPath, token)
                    #sync_status(clusterId, table, 'POST', setInSync)
                    #update state table - which was out of sync - with inSync True
                    status, rc = probe(
                        f'{endpointPath}/update', 
                        'POST', 
                        {'set': {
                            'state': 'loaded', 'inSync': True},
                            'where': {'uuid': endpoint, 'tableName': 'state'}
                        },
                        token=token
                    )
                    log(f"{job} {table} - #SYNC marking outOfSync endpoint for table state as inSync on the outOfSync, to be in line with other tables - {status} {rc}")

            except Exception as e:
                log_exception(job, table, e)
                log(f"{job} {table} - #SYNC Worker rolling back pause / inSync")
                setInSync = {tableEndpoint: {'inSync': False, 'state': 'loaded'}}
                statusResult, rc = sync_status(clusterId, table, 'POST', setInSync)
                #UnPause
                message, rc = table_cutover(clusterId, table, 'stop')
                return log_exception(job, table, e), 500

            # Un-Pause
            log(f"{job} {table} - #SYNC Worker -  completes cutover by un-pausing table")
            message, rc = table_cutover(clusterId, table, 'stop')

        message = f'{job} {table} - #SYNC finished syncing {endpoint[0]} for table {table} in cluster {cluster}'
        log(message)
    return {"message": message}, 200
    
def get_and_run_job(path):
    job, rc = probe(path,'POST', {'node': nodeId}, timeout=30.0, auth='cluster') # TODO - Parameterize timeout
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
        set_db_env(args[-1])
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