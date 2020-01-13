import sys, time, requests, os
if 'PYQL_TYPE' in os.environ:
    if os.environ['PYQL_TYPE'] == 'K8S':
        # Processing environ variables for Kubernetes implementation
        hostAddr = '-'.join(socket.gethostbyname(socket.gethostname()).split('.'))
        k8sNamespace = os.environ['K8S_NAMESPACE']
        k8sCluster = os.environ['K8S_CLUSTER']
        k8sDomain = f"pyql-cluster.{k8sNamespace}.svc.{k8sCluster}"
        # Setting PYQL_NODE - used when joining a cluster
        os.environ['PYQL_NODE'] = f"{hostAddr}.{k8sDomain}"



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
        start = delay - 5
        while True:
            time.sleep(1)
            if delay < time.time() - start:
                try:
                    message, rc = probe(endpoint)
                except Exception as e:
                    print(f"checker.py error in checking probe rc - {repr(e)}")
                    continue
                if rc:
                    if not rc == 200:
                        message, rc = probe(action)
                        print(f"checker.py probe rc -({message} {rc}")
                start = time.time()