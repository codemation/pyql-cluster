import sys, time, requests, json, os

clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']
nodePath = f'http://{os.environ["PYQL_NODE"]}:{os.environ["PYQL_PORT"]}'

def probe(path, method='GET', data=None):
    url = f'{nodePath}{path}' if not 'http' in path else path
    if method == 'GET':
        r = requests.get(url, headers={'Accept': 'application/json'})
    else:
        r = requests.post(url, headers={'Accept': 'application/json', "Content-Type": "application/json"}, data=json.dumps(data))
    try:
        return r.json(),r.status_code
    except:
        return r.text, r.status_code

def get_requirement(job):
    if 'jobRequires' in job:
        for req in job['jobRequires']:
            if job['jobRequires'][req]['type'] == 'job':
                requirement = get_requirement(job['jobRequires'][req])
                return probe(job['path'], job['method'], requirement)[0]
            if job['jobRequires'][req]['type'] == 'json':
                return json.loads(job['jobRequires'][req]['json'])
    else:
        return None

"""
expeceted job structure
    job = {
        'job': 'updateTableModTime',
        'jobType': 'cluster',
        'method': 'POST',
        'path': '/cluster/pyql/table/state/update'
        'data': {..}
    }
""" 
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
        print(f"error adding {job['job']} to {path} queue, error: {message}")
    return message,rc

def get_and_process_job(path):
    job, rc = probe(path)
    if not "message" in job:
        jobId = job['id']
        job = job['config']
        print(f"pulled {job} with {path} with rc {rc}")
        try:
            if job['jobType'] == 'cluster':
                #Distribute to cluster job queue
                print(f"adding job {job} to cluster queue")
                message, rc = add_job_to_queue('/cluster/jobs/add', job)
            elif job['jobType'] == 'node':
                message, rc = probe(f"{nodePath}{job['path']}", job['method'], job['data'])
            elif job['jobType'] == 'tablesync':
                print(f"adding job {job} to tablesync queue")
                message, rc = add_job_to_queue('/cluster/syncjobs/add', job)
            else:
                message, rc =  f"{job['job']} is missing jobType field", 200
            try:
                probe(f'/internal/job/{jobId}/finished', 'POST')
            except Exception as e:
                print(f"{os.environ['HOSTNAME']} worker.py encountered exception finishing job, need to cleanup {jobId} later")
                probe(f'/internal/job/{jobId}/queued', 'POST')
        except Exception as e:
            print(f"{os.environ['HOSTNAME']} worker.py encountered exception hanlding job {job} - add back to queue")
            probe(f'/internal/job/{jobId}/queued', 'POST')
        return message,rc
    return job,rc
print(__name__)
if __name__== '__main__':
    args = sys.argv
    print(len(args))
    if len(args) > 2:
        jobpath, delay  = args[1], float(args[2])
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