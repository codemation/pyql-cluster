"""
    Cluster Job Worker
"""

import sys, time, requests, json, os

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

def get_and_process_job(path):
    job, rc = probe(path,'POST', {'node': os.environ['PYQL_NODE']})
    def process_job(job, rc):
        if rc == 200 and not 'message' in job:
            print(f"job pulled {job}")
            if job['config']['jobType'] == 'cluster' or  job['config']['jobType'] == 'cron':
                print(f'cluster jobworker - running {job["config"]["path"]}')
                set_job_status(job['id'],job['config']['jobType'],'running', message=f"starting {job['config']['job']}")
                try:
                    job['config']['data'] == None if not 'data' in job['config'] else job['config']['data']
                    message, rc = probe(f'{clusterSvcName}{job["config"]["path"]}', job['config']['method'], job['config']['data'])
                    if rc == 200:
                        if 'runAfter' in job:
                            message2, rc2 = process_job(job['runAfter'])
                            message = {'message': message, 'runAfter': message2}
                        set_job_status(job['id'], job['config']['jobType'],'finished')
                    else:
                        set_job_status(job['id'], job['config']['jobType'],'queued')
                except Exception as e:
                    print(repr(e))
                    set_job_status(job['id'], job['config']['jobType'],'queued', lastError=str(repr(e)))
            else:
                set_job_status(job['id'],job['config']['jobType'], 'queued') #TODO - Should fail job
                message, rc =  f'{job["id"]} is missing jobType field', 200
                print(message)
            return message,rc
        return job,rc
    return process_job(job,rc)

if __name__=='__main__':
    args = sys.argv
    if len(args) > 2:
        jobpath, delay  = args[1], float(args[2])
        start = time.time()
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                result, rc = get_and_process_job(f'{clusterSvcName}{jobpath}')
                start = time.time()
                