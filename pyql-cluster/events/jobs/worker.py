import sys, time, requests, json, os, logging


def log(log):
    log = f" {os.environ['HOSTNAME']} - internal-worker - {log}"
    logging.warning(log)
    return log

NODE_IP = os.environ.get('PYQL_NODE')

if os.environ.get('PYQL_TYPE') in ['K8S', 'DOCKER']:
    import socket
    NODE_IP = socket.gethostbyname(socket.getfqdn())

session = requests.Session()

def set_db_env(path):
    sys.path.append(path)
    import pydb
    db = pydb.get_db()
    db._run_async_tasks(db.load_tables())
    global env
    env = db.tables['env']

CLUSTER_SVC_NAME = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'
NODE_PATH = f'http://{NODE_IP}:{os.environ["PYQL_PORT"]}'

def probe(path, method='GET', data=None, auth=None, **kw):
    action = requests if not 'session' in kw else kw['session']
    path = f'{path}'   
    auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
    headers = {
        'Accept': 'application/json', "Content-Type": "application/json",
        "Authentication": f"Token {env[auth] if not 'token' in kw else kw['token']}"}
    if method == 'GET':
        r = requests.get(path, headers=headers)
    else:
        r = requests.post(path, headers=headers, data=json.dumps(data))
    try:
        return r.json(),r.status_code
    except Exception as e:
        return r.text, r.status_code


def add_job_to_queue(path, job, **kw):
    try:
        message, rc = probe(f'{CLUSTER_SVC_NAME}{path}', 'POST', job, **kw)
    except Exception as e:
        message = {"message": f"encountered exception {repr(e)} with {CLUSTER_SVC_NAME}{path} for  job {job}"}
        logging.exception(log(message))
        rc = 500
    
    if rc == 200:
        log(f"added {job['job']} to {path} queue")
    else:
        log(f"error adding {job['job']} to {path} queue, error: {message} {rc}")
    return message,rc

def get_and_process_job(path):
    try:
        job, rc = probe(f'{NODE_PATH}{path}', auth='local', session=session)
    except Exception as e:
        print(f"worker.py - Error probing {path}, try again later")
        return {"message": f"worker.py - Error probing {path}"}, 400
    if not "message" in job:
        print(f"pulled job {job} with {path} with rc {rc}")
        job_id = job['id']
        job = job['config']
        try:
            if job['job_type'] == 'cluster':
                #Distribute to cluster job queue
                
                if 'join_cluster' in job['job']: # need to use join_token
                    log(f"join cluster job {job}, attempting to join")
                    message, rc = probe(f"{CLUSTER_SVC_NAME}{job['path']}", job['method'], job['data'], token=job['join_token'], timeout=30)
                else:
                    log(f"adding job {job} to cluster jobs queue")
                    message, rc = probe(f"{CLUSTER_SVC_NAME}/cluster/jobs/add", 'POST', job, timeout=30)
                log(f"finished adding job {job} to cluster jobs queue {message} {rc}")
            elif job['job_type'] == 'node':
                auth = 'local' if not 'init_cluster' in job['job'] else 'cluster'
                message, rc = probe(f"{NODE_PATH}{job['path']}", job['method'], job['data'], auth=auth, session=session)
            elif job['job_type'] == 'tablesync':
                log(f"adding job {job} to tablesync queue")
                message, rc = add_job_to_queue(f'/cluster/syncjobs/add', job)
            else:
                message, rc =  f"{job['job']} is missing job_type field", 200
            if not rc == 200:
                probe(f'{NODE_PATH}/internal/job/{job_id}/queued', 'POST', auth='local', session=session)
            else:
                try:
                    probe(f'{NODE_PATH}/internal/job/{job_id}/finished', 'POST', auth='local', session=session)
                except Exception as e:
                    warning = f'encountered exception finishing job, need to cleanup {job_id} later'
                    logging.exception(log(warning))
                    probe(f'{NODE_PATH}/internal/job/{job_id}/queued', 'POST', auth='local', session=session)
        except Exception as e:
            message = f"{os.environ['HOSTNAME']} worker.py encountered exception {repr(e)} hanlding job {job} - add back to queue"
            logging.exception(log(message))
            probe(f'{NODE_PATH}/internal/job/{job_id}/queued', 'POST', auth='local', session=session)
        return message,rc
    return job,rc
print(__name__)
if __name__== '__main__':
    args = sys.argv
    print(len(args))
    if len(args) > 2:
        jobpath, delay  = args[1], float(args[2])
        set_db_env(args[-1])
        log(f"starting worker for monitoring {jobpath} with delay of {delay}")
        start = time.time() - 5
        while True:
            delayed = time.time() - start
            if delay < delayed:
                try:
                    result, rc = get_and_process_job(jobpath)
                except Exception as e:
                    logging.exception(log(f"error runnng get_and_process_job using {jobpath}"))
                start = time.time()
                continue
            time.sleep(delay - delayed)