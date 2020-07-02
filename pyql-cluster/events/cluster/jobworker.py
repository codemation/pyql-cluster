"""
    Cluster Job Worker
"""

import sys, datetime, time, requests, json, os, logging

logging.basicConfig()

#CLUSTER_SVC_NAME = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'
#CLUSTER_SVC_NAME = 'http://localhost'

NODE_IP = os.environ.get('PYQL_NODE')

if os.environ.get('PYQL_TYPE') in ['K8S', 'DOCKER']:
    import socket
    NODE_IP = socket.gethostbyname(socket.getfqdn())

CLUSTER_SVC_NAME = f'http://{NODE_IP}:{os.environ["PYQL_PORT"]}'
session = requests.Session()


def set_db_env(path):
    sys.path.append(path)
    import pydb
    database = pydb.get_db()
    print(database.tables)
    global env
    env = database.tables['env']
    global nodeId
    nodeId = env['PYQL_ENDPOINT']

def probe(path, method='GET', data=None, auth=None, timeout=300.0, **kw):
    path = f'{path}'   
    auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
    headers = {
        'Accept': 'application/json', "Content-Type": "application/json",
        "Authentication": f"Token {env[auth] if not 'token' in kw else kw['token']}"}
    if method == 'GET':
        r = session.get(path, headers=headers,
                timeout=timeout)
    else:
        r = session.post(path, headers=headers,
                data=json.dumps(data) if not data == None else data, 
                timeout=timeout)
    try:
        return r.json(),r.status_code
    except Exception as e:
        return r.text, r.status_code



def set_job_status(jobId, jobtype, status, **kwargs):
    # Need to mark job finished/queued - # Using - /cluster/<jobtype>/<uuid>/<status>
    return probe(
        f"{CLUSTER_SVC_NAME}/cluster/job/{jobtype}/{jobId}/{status}",
        'POST',
        kwargs)
def log(log):
    log = f" {os.environ['HOSTNAME']} - jobworker - {log}"
    logging.warning(log)
    return log

def get_and_process_job(path):
    job, rc = probe(path,'POST', {'node': nodeId})
    def process_job(job, rc):
        if not rc == 200 or 'message' in job:
            return job,rc
        log(f"job pulled {job['name']}")
        job_config = job['config']
        job_type = job_config['job_type']

        log(f'cluster jobworker - running {job_config["path"]}')
        set_job_status(job['id'], job_type, 'running', message=f"starting {job_config['job']}")

        try:
            # prepare job config
            url = f"{CLUSTER_SVC_NAME if not 'node' in job_config else job_config['node']}{job_config['path']}"
            job_data = None if not 'data' in job_config else job_config['data']
            config = {
                'method': job_config['method'], 
                'data': job_data}

            # Execute Job # 
            message, rc = probe(url, **config)
            if not rc == 200:
                # Re queuing job
                log(f"job {job} response {message} rc {rc}")
                set_job_status(job['id'], job_type,'queued')

            # Job was successfully run
            log(f"job {job['name']} response {message} rc {rc} marking finished")

            # Running next job if there is one waiting on current job
            set_job_status(job['id'], job_type,'finished')
            if 'nextJob' in job_config:
                set_job_status(job_config['nextJob'], 'jobs','queued')

        except Exception as e:
            log(f"Exception when proceessing job {job} {repr(e)}")
            set_job_status(job['id'], job_type, 'queued', last_error=f"Exception when proceessing job")

        return message,rc       
    return process_job(job,rc)

if __name__=='__main__':
    args = sys.argv
    if len(args) > 2:
        jobpath, delay  = args[1], float(args[2])
        set_db_env(args[-1])
        print(f"jobworker.py started with endpoint {nodeId} path {jobpath}")
        start = time.time() - 5
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                try:
                    result, rc = probe(f'{CLUSTER_SVC_NAME}{jobpath}', method='POST')
                    #result, rc = get_and_process_job(f'{CLUSTER_SVC_NAME}{jobpath}')
                except Exception as e:
                    logging.exception(f"Exception when running / processing")
                start = time.time()
                