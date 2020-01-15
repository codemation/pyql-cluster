"""
    Cluster Job Worker
"""

import sys, time, requests, json, os

if 'PYQL_NODE' in os.environ:
    nodeIP = os.environ['PYQL_NODE']

if 'PYQL_TYPE' in os.environ:
    if os.environ['PYQL_TYPE'] == 'K8S':
        import socket
        nodeIp = socket.gethostbyname(socket.getfqdn())

clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'

def probe(path, method='GET', data=None):
    url = f'{path}'
    if method == 'GET':
        r = requests.get(url, headers={'Accept': 'application/json'})
    else:
        r = requests.post(url, headers={'Accept': 'application/json', "Content-Type": "application/json"}, data=json.dumps(data))
    try:
        return r.json(),r.status_code
    except:
        return r.text, r.status_code
def set_job_status(jobId, jobtype, status, **kwargs):
    # Need to mark job finished/queued - # Using - /cluster/<jobtype>/<uuid>/<status>
    return probe(
        f"{clusterSvcName}/cluster/{jobtype}/{jobId}/{status}",
        'POST',
        kwargs
        )
def log(log):
    print(f"{os.environ['HOSTNAME']} jobworker - {log}")

def get_and_process_job(path):
    job, rc = probe(path,'POST', {'node': nodeIP})
    def process_job(job, rc):
        if rc == 200 and not 'message' in job:
            log(f"job pulled {job}")
            if job['config']['jobType'] == 'cluster' or  job['config']['jobType'] == 'cron':
                log(f'cluster jobworker - running {job["config"]["path"]}')
                set_job_status(job['id'],job['config']['jobType'],'running', message=f"starting {job['config']['job']}")
                try:
                    job['config']['data'] == None if not 'data' in job['config'] else job['config']['data']
                    url = f"{clusterSvcName if not 'node' in job['config'] else job['config']['node']}{job['config']['path']}"
                    message, rc = probe(url, job['config']['method'], job['config']['data'])
                    if rc == 200:
                        if 'runAfter' in job:
                            message2, rc2 = process_job(job['runAfter'])
                            message = {'message': message, 'runAfter': message2}
                        log(f"job {job} response {message} rc {rc} marking finished")
                        set_job_status(job['id'], job['config']['jobType'],'finished')
                        if 'nextJob' in job['config']:
                            set_job_status(job['config']['nextJob'], 'jobs','queued')
                    else:
                        log(f"job {job} response {message} rc {rc}")
                        set_job_status(job['id'], job['config']['jobType'],'queued')
                except Exception as e:
                    log(f"Exception when proceessing job {job} {repr(e)}")
                    set_job_status(job['id'], job['config']['jobType'],'queued', lastError=str(repr(e)))
            else:
                set_job_status(job['id'],job['config']['jobType'], 'queued')
                message, rc =  f'{job["id"]} is missing jobType field', 200
                print(message)
            return message,rc
        return job,rc
    return process_job(job,rc)

if __name__=='__main__':
    args = sys.argv
    if len(args) > 2:
        jobpath, delay  = args[1], float(args[2])
        start = time.time() - 5
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                try:
                    result, rc = get_and_process_job(f'{clusterSvcName}{jobpath}')
                except Exception as e:
                    print(f"Exception when running / processing")
                start = time.time()
                