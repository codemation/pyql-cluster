"""
    simple jobs that run at set intervals
"""
#TODO - This module is not actually running

import sys, time, requests, json, os

clusterSvcName = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'

def probe(path, method='GET', data=None):
    url = f'{clusterSvcName}{path}'
    if method == 'GET':
        r = requests.get(url, headers={'Accept': 'application/json'})
    else:
        r = requests.post(url, headers={'Accept': 'application/json', "Content-Type": "application/json"}, data=data)
    try:
        return r.json(),r.status_code
    except:
        return r.text, r.status_code

if __name__=='__main__':
    args = sys.argv
    if len(args) > 3:
        jobpath, method, delay, startDelay  = args[1], args[2], float(args[3]), int(args[4])
        start = delay
        time.sleep(startDelay)
        while True:
            if delay < time.time() - start:
                result, rc = probe(jobpath, method)
                start = time.time()
                