import sys, time, requests, os

if 'PYQL_NODE' in os.environ:
    NODE_IP = os.environ['PYQL_NODE']

if 'PYQL_TYPE' in os.environ:
    if os.environ['PYQL_TYPE'] == 'K8S' or os.environ['PYQL_TYPE'] == 'DOCKER':
        import socket
        NODE_IP = socket.gethostbyname(socket.getfqdn())

session = requests.Session()
NODE_PORT = os.environ['PYQL_PORT']

def set_db_env(path):
    sys.path.append(path)
    import pydb
    db = pydb.get_db()
    db._run_async_tasks(db.load_tables())
    global env
    env = db.tables['env']

def probe(path, method='GET', data=None, auth=None):
    path = f'http://{NODE_IP}:{NODE_PORT}{path}'
    auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
    headers = {
        'Accept': 'application/json', "Content-Type": "application/json",
        "Authentication": f"Token {env[auth]}"}
    if method == 'GET':
        r = session.get(path, headers=headers)
    else:
        r = session.post(path, headers=headers, data=json.dumps(data))
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