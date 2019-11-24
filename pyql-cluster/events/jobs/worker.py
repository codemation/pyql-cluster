import sys, time, requests, json

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
    
def get_and_run_job(path):
    job, rc = probe(endpoint)
    if not "message" in job:
        # Jobs pulled
        print(f"preparing to run {job}")
        # check for job requirements
        result, rc = probe(job['path'], job['method'], get_requirement(job))
        return result, rc
    return job, rc
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