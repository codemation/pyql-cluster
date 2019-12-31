import sys, time, requests, os

def probe(endpoint):
    url = f'http://{os.environ["PYQL_NODE"]}:{os.environ["PYQL_PORT"]}{endpoint}'
    r = requests.get(url, headers={'Accept': 'application/json'})
    try:
        return r.json(),r.status_code
    except:
        return r.text, r.status_code
if __name__=='__main__':
    args = sys.argv
    if len(args) > 3:
        endpoint, delay, action  = args[1], float(args[2]), args[3]
        start = delay
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                message, rc = probe(endpoint)
                print(f"checker.py probe rc -  {rc}")
                if not rc == 200:
                    message, rc = probe(action)
                    print(f"checker.py probe rc -({message} {rc}")
                start = time.time()