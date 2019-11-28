"""
    Cluster Job Worker
"""

import sys, time, requests, json

clusterSvcName = 'http://localhost:8090' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']

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


def get_and_process_job(path):
    job, rc = probe(endpoint)
    def process_job(job):
        if not "message" in job:
            if job['jobType'] == 'cluster':
                message, rc = probe(f'{clusterSvcName}/{job['path']}', job['method'], job['data'])
                if rc == 200:
                    if 'runAfter' in job:
                        message2, rc2 = process_job(job['runAfter'])
                        message = {'message': message, 'runAfter': message2}
            else:
                message, rc =  f'{job['job']} is missing jobType field'), 200
            return message,rc
        return job,rc
    return process_job(job)

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
                