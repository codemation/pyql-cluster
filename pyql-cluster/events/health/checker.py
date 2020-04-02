import sys, time, requests, os

if 'PYQL_NODE' in os.environ:
    nodeIP = os.environ['PYQL_NODE']

if 'PYQL_TYPE' in os.environ:
    if os.environ['PYQL_TYPE'] == 'K8S':
        import socket
        nodeIP = socket.gethostbyname(socket.getfqdn())
nodePort = os.environ['PYQL_PORT']

def set_db_env(path):
    sys.path.append(path)
    import pydb
    database = pydb.get_db()
    global env
    env = database.tables['env']

def probe(path, method='GET', data=None, auth=None):
    path = f'http://{nodeIP}:{nodePort}{path}'
    auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
    headers = {
        'Accept': 'application/json', "Content-Type": "application/json",
        "Authentication": f"Token {env[auth]}"}
    if method == 'GET':
        r = requests.get(path, headers=headers,
                timeout=1.0)
    else:
        r = requests.post(path, headers=headers,
                data=json.dumps(data), timeout=1.0)
    try:
        return r.json(),r.status_code
    except Exception as e:
        return r.text, r.status_code

if __name__=='__main__':
    args = sys.argv
    if len(args) > 3:
        endpoint, delay, action  = args[1], float(args[2]), args[3]
        set_db_env(args[-1])
        start = delay - 5
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                try:
                    message, rc = probe(endpoint,auth='local')
                except Exception as e:
                    print(f"checker.py error in checking probe rc - {repr(e)}")
                    continue
                if rc:
                    if not rc == 200:
                        message, rc = probe(action, auth='local')
                        print(f"checker.py probe rc -({message} {rc}")
                start = time.time()