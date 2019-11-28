import sys, time, requests, json, os

clusterSvcName = 'http://localhost:8090' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']
nodePath = 'http://localhost:8080'

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
def add_job_cluster_queue(job):
    message, rc = probe(f'{clusterSvcName}/cluster/jobs', 'POST', job)
    if rc == 200:
        print(f"added {job['job']} to cluster jobs queue")
    else:
        print(f"error adding {job['job']} to cluster jobs queue, error: {message}")

def get_and_process_job(path):
    job, rc = probe(endpoint)
    if not "message" in job:
        if job['jobType'] == 'cluster':
            #Distribute to cluster job queue
            message, rc = add_job_cluster_queue(job)
        elif job['jobType']  == 'node':
            message, rc = probe(f'{nodePath}/{job['path']}', job['method'], job['data'])
        else:
            message, rc =  f'{job['job']} is missing jobType field'), 200
        return message,rc
    return job,rc
if __name__=='__main__':
    args = sys.argv
    if len(args) > 3:
        jobpath, delay  = args[1], float(args[2])
        start = delay
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                result, rc = get_and_process_job(jobpath)
                start = time.time()