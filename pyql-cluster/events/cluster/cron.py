"""
    simple jobs that run at set intervals
"""

import sys, time, requests, json

clusterSvcName = 'http://localhost:8080' # TODO: replace with clusterSvcName = os.environ['CLUSTER_SVC_NAME']

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

if __name__=='__main__':
    args = sys.argv
    if len(args) > 3:
        jobpath, delay  = args[1], float(args[2])
        start = delay
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                result, rc = get_and_run_job(jobpath)
                start = time.time()
                