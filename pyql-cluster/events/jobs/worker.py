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
    message, rc = probe(f'{clusterSvcName}{path}', 'POST', job)
    print(message)
    if rc == 200:
        print(f"added {job['job']} to {path} queue")
    else:
        print(f"error adding {job['job']} to {path} queue, error: {message}")

def get_and_process_job(path):
    job, rc = probe(path)
    if not "message" in job:
        if job['jobType'] == 'cluster':
            #Distribute to cluster job queue
            print(f"adding job {job} to cluster queue")
            message, rc = add_job_to_queue('/cluster/jobs/add', job)
        elif job['jobType'] == 'node':
            message, rc = probe(f"{nodePath}{job['path']}", job['method'], job['data'])
        elif job['jobType'] == 'tablesync':
            message, rc = add_job_to_queue('/cluster/syncjobs', job)
        else:
            message, rc =  f"{job['job']} is missing jobType field", 200
        return message,rc
    return job,rc
print(__name__)
if __name__== '__main__':
    args = sys.argv
    print(len(args))
    if len(args) > 2:
        jobpath, delay  = args[1], float(args[2])
        print(f"starting worker for monitoring {jobpath} with delay of {delay}")
        start = delay
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                result, rc = get_and_process_job(jobpath)
                start = time.time()