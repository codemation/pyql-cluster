import sys, 
"""TODO - Delete later
if 'PYQL_TYPE' in os.environ:
    if os.environ['PYQL_TYPE'] == 'K8S':
        import socket
        os.environ['PYQL_NODE'] = socket.getfqdn()

if 'PYQL_NODE' in os.environ:
    nodeIP = os.environ['PYQL_NODE']
"""
if 'PYQL_TYPE' in os.environ:
    if os.environ['PYQL_TYPE'] == 'K8S':
        import socket
        nodeIP = socket.gethostbyname(socket.getfqdn())

def set_db_env(path):
    sys.path.append(path)
    import pydb
    database = pydb.get_db()
    global env
    env = database.tables['env']

clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'
nodePath = f'http://{nodeIP}:{os.environ["PYQL_PORT"]}'

def probe(path, method='GET', data=None, auth=None, **kw):
    path = f'{path}'   
    auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
    headers = {
        'Accept': 'application/json', "Content-Type": "application/json",
        "Authentication": f"Token {env[auth] if not 'token' in kw else kw['token']}"}
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


def add_job_to_queue(path, job):
    try:
        message, rc = probe(f'{clusterSvcName}{path}', 'POST', job)
    except Exception as e:
        message = f"{os.environ['HOSTNAME']} worker.py encountered exception {repr(e)} with {clusterSvcName}{path} for  job {job}"
        rc = 500
        print(message)
    
    if rc == 200:
        print(f"added {job['job']} to {path} queue")
    else:
        print(f"error adding {job['job']} to {path} queue, error: {message} {rc}")
    return message,rc

def get_and_process_job(path):
    try:
        job, rc = probe(f'{nodePath}{path}', auth='local')
    except Exception as e:
        print(f"worker.py - Error probing {path}, try again later")
        return {"message": f"worker.py - Error probing {path}"}, 400
    if not "message" in job:
        print(f"pulled job {job} with {path} with rc {rc}")
        jobId = job['id']
        job = job['config']
        try:
            if job['jobType'] == 'cluster':
                #Distribute to cluster job queue
                print(f"adding job {job} to cluster jobs queue")
                if 'joinCluster' in job['job']: # need to use joinToken
                    message, rc = probe(f"{clusterSvcName}{job['path']}", job['method'], job['data'], token=job['joinToken'])
                else:
                    message, rc = probe(f"{clusterSvcName}/cluster/pyql/jobs/add", 'POST', job)
                print(f"finished adding job {job} to cluster jobs queue {message} {rc}")
            elif job['jobType'] == 'node':
                auth = 'local' if not 'initCluster' in job['job'] else 'cluster'
                message, rc = probe(f"{nodePath}{job['path']}", job['method'], job['data'], auth=auth)
            elif job['jobType'] == 'tablesync':
                print(f"adding job {job} to tablesync queue")
                message, rc = add_job_to_queue(f'/cluster/pyql/syncjobs/add', job)
            else:
                message, rc =  f"{job['job']} is missing jobType field", 200
            if not rc == 200:
                probe(f'{nodePath}/internal/job/{jobId}/queued', 'POST', auth='local')
            else:
                try:
                    probe(f'{nodePath}/internal/job/{jobId}/finished', 'POST', auth='local')
                except Exception as e:
                    print(f"{os.environ['HOSTNAME']} worker.py encountered exception finishing job, need to cleanup {jobId} later")
                    probe(f'{nodePath}/internal/job/{jobId}/queued', 'POST', auth='local')
        except Exception as e:
            print(f"{os.environ['HOSTNAME']} worker.py encountered exception hanlding job {job} - add back to queue")
            probe(f'{nodePath}/internal/job/{jobId}/queued', 'POST', auth='local')
        return message,rc
    return job,rc
print(__name__)
if __name__== '__main__':
    args = sys.argv
    print(len(args))
    if len(args) > 2:
        jobpath, delay  = args[1], float(args[2])
        set_db_env(args[-1])
        print(f"starting worker for monitoring {jobpath} with delay of {delay}")
        start = time.time() - 5
        while True:
            delayed = time.time() - start
            if delay < delayed:
                try:
                    result, rc = get_and_process_job(jobpath)
                except Exception as e:
                    print(repr(e))
                start = time.time()
                continue
            time.sleep(delay - delayed)