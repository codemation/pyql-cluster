import sys, time, requests

def probe(endpoint):
    """
        default staring is http://localhost:8080
    """
    url = f'http://localhost:8080{endpoint}'
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
                print(rc)
                if not rc == 200:
                    message, rc = probe(action)
                    print(message, rc)
                start = time.time()