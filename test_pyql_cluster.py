import os, unittest, json, requests, time, random

class cluster:
    def __init__(self, **kw):
        self.steps = 0
        self.session = requests.Session()
        # loads 
        # {'clusterPort': 8090, 'clusterIp': '192.168.3.33', 'initAdminPw': 'YWRtaW46YWJjZDEyMzQ='}
        self.nodes = []
        with open('pyql-cluster/test_config.json', 'r') as config:
            self.config = json.loads(config.read())
        self.debug = True if 'debug' in kw else False
    def step(self, action):
        print(f'starting step {self.steps} - {action}')
        self.steps+=1
    def auth_setup(self):
        # Test Basic auth by pulling token from /auth/token/cluster
        self.step('test_auth - waiting for cluster to finish initalizing')
        time.sleep(10)
        self.step('test_auth - trying to pull clusterToken')
        token, rc = self.probe(
            f'/auth/token/cluster',
            auth={
                'method': 'basic', 'auth': self.config['initAdminPw']
            }
        )
        print(f"token {token} rc {rc}")
        self.clusterToken = token['PYQL_CLUSTER_SERVICE_TOKEN']
        print(f"token pulled {self.clusterToken}")
    def docker_stop(self, port):
        assert port in self.nodes, f"cannot stop node pyql-cluster-{port}, not in list of started nodes"
        os.system(f"docker container stop pyql-cluster-{port}")
    def docker_restart(self, port):
        assert port in self.nodes, f"cannot restart node pyql-cluster-{port}, not in list of started nodes"
        #os.system(f"docker container start pyql-cluster-{port}")
        self.expand_cluster(port=port, join='rejoin')
    def reset(self):
        os.system("""for cont in $(docker container ls | grep 'pyql' | awk '{print $1}'); do docker container rm $(docker container stop $cont); done""")
        for node in self.nodes:
            os.system(f'docker container rm $(docker container stop pyql-cluster-{node}) || docker container rm pyql-cluster-{node}')
    def init_cluster(self):
        self.reset()
        self.nodes = [self.config['clusterPort']]
        host = self.config['clusterIp']
        port = self.config['clusterPort']
        debug = '--debug' if self.debug == True else ''
        os.system(f"./restart_pyql_cluster.sh dryrun.0.0 {port} {host} {port} init --no-cache {debug}")
    def expand_cluster(self, token=None, port=None, join='join'):
        if token == None:
            token, rc = self.probe(
                f'/auth/token/join',
                auth={
                    'method': 'basic', 'auth': self.config['initAdminPw']
                }
            )
            assert isinstance(token, dict), f"expected token is type dict, found {type(token)} with value {token}"
            token = token['join']
        if port == None:
            self.nodes.append(self.nodes[-1]+1)
            port = self.nodes[-1]
            print(f"expanding cluster using port {port}")
        clusterHost = self.config['clusterIp']
        clusterPort = self.config['clusterPort']
        debug = '--debug' if self.debug == True else ''
        
        os.system(f"./restart_pyql_cluster.sh dryrun.0.0 {port} {clusterHost} {clusterPort} {join} {token} '' {debug}")
    def verify_data(self, tryCount=0):
        # for each cluster;
        clusters, rc = self.probe('/cluster/pyql/table/clusters/select')
        print(f"clusters {clusters}")
        for cluster in clusters['data']:
            # for each table in clusters
            tables, rc = self.probe(
                '/cluster/pyql/table/tables/select',
                method='POST',
                data={
                    'select': ['name'],
                    'where': {'cluster': cluster['id']}
                }
            )
            verify = {}
            #print(f"tables {tables}")
            for tb in tables['data']:
                table = tb['name']
                dataToVerify = {}
                # for each table endpoint - verify data
                tableEndpoints, rc = self.probe(f"/cluster/{cluster['id']}/table/{table}/endpoints")
                #print(f"tableEndpoints - {tableEndpoints}")
                for endpoint in tableEndpoints['inSync']:
                    endpointInfo = tableEndpoints['inSync'][endpoint]
                    dataToVerify[endpoint], rc = probe(
                        f"http://{endpointInfo['path']}/db/{endpointInfo['dbname']}/table/{table}/select",
                        auth={  
                            'method': 'token',
                            'auth': endpointInfo['token']
                        },
                        session=self.session
                    )
                    if rc == 200:
                        dataToVerify[endpoint] = dataToVerify[endpoint]['data']
                        #print(f"dataToVerify {dataToVerify[endpoint]}")
                    else:
                        assert False, f"dataToVerify ERROR  when acessing endpoint: {endpointInfo} table: {table} -- {dataToVerify[endpoint]}"
                verify[table] = {}
                for endpoint in dataToVerify:
                    verify[table][endpoint] = {'status': [], 'diff': {}}
                    for ep in dataToVerify:
                        if ep == endpoint:
                            continue
                        for t1r, t2r, in zip(dataToVerify[endpoint], dataToVerify[ep]):
                            if not t1r == t2r:
                                if not ep in verify[table][endpoint]['diff']:
                                    verify[table][endpoint]['diff'][ep] = []
                                verify[table][endpoint]['diff'][ep].append((t1r, t2r))
                                # every subsequent row will not be equal from here
                                break
                        if ep in verify[table][endpoint]['diff']:
                            verify[table][endpoint]['status'].append(False)
                            continue
                        verify[table][endpoint]['status'].append(True)
            
            verifyFail = []
            for table, endpoints in verify.items():
                for endpoint, status in endpoints.items():
                    if not table == 'jobs':
                        if False in status['status']: 
                            if tryCount < 2:
                                print(f"{table} endpoint {endpoint} data did not match with {status['diff'].keys()} - retrying")
                                time.sleep(5)
                                self.verify_data(tryCount+1)
                                return # avoid asserting if this is not the max retry run
                        if False in status['status']:
                            verifyFail.append(f"{table} endpoint {endpoint} data did not match with {status['diff']}")
            assert len(verifyFail) == 0, f"verification failed on endpoint(s) - {verifyFail}"
            print(f"verify completed for cluster {cluster['name']} - {verify}")
            #assert not False in status['status'], f"{table} endpoint {endpoint} data did not match with {status['diff']}"


    def get_cluster_jobs(self, jobType=None):
        if jobType == None:
            return self.probe(
                '/cluster/pyql/table/jobs/select', 
                auth={'method': 'token', 'auth': self.clusterToken})
        return self.probe(
            '/cluster/pyql/table/jobs/select', method='POST',
            data={
                'select': ['*'], 'where': {
                    'type': jobType
                }
            },
            auth={'method': 'token', 'auth': self.clusterToken})
    def probe(self, path, **kw):
        clusterIp = self.config['clusterIp']
        clusterPort = self.config['clusterPort']
        kw['auth'] = {'method': 'token', 'auth': self.clusterToken} if not 'auth' in kw else kw['auth']
        kw['session'] = self.session
        return probe(f"http://{clusterIp}:{clusterPort}{path}", **kw)
    def sync_job_check(self):
        # checking for sync jobs
        maxCheck = 240 # should take less than 60 seconds for new sync jobs 
        start = time.time()
        while time.time() - start < maxCheck:
            jobs, rc = self.get_cluster_jobs()
            if rc == 200:
                jobs = [ job['type'] for job in jobs['data'] ]
            if 'syncjobs' in jobs:
                break
            print(f"waiting for sync jobs to start {jobs} - {time.time() - start:.2f}")
            time.sleep(5)
        assert 'syncjobs' in jobs, f"should take less than {maxCheck} seconds for new sync jobs"

        self.step('syncjobs detected, waiting for pyql tables to sync')
        time.sleep(10)
        maxSyncRunTimePerJob = 300
        startTime = time.time()
        lastCount = len(self.get_cluster_jobs('syncjobs')[0]['data'])
        lastJob = None
        while time.time() - startTime < maxSyncRunTimePerJob:
            jobs, rc = self.get_cluster_jobs('syncjobs')
            if rc == 200:
                if len(jobs['data']) < lastCount or len(jobs['data']) > lastCount:
                    lastCount = len(jobs['data'])
                    startTime = time.time()
                if len(jobs['data']) == 1:
                    if lastJob == jobs['data'][0]['id']:
                        continue
                    else:
                        lastJob = jobs['data'][0]['id']
                        lastCount = len(jobs['data'])
                        startTime = time.time()
                if lastCount == 0:
                    break
            print(f"waiting for {lastCount} sync jobs to complete {time.time() - startTime:.2f} sec")
            time.sleep(5)
        assert lastCount == 0, f"waited too long on a syncjobs job to finish - {time.time() - startTime:.2f}, {jobs}"
    def test_node_recovery(self, count):
        self.step(f"bring down {count} random nodes to verify node removal & recovery upon restarting")
        stoppedNodes = []
        nodes = [n for n in self.nodes]
        for i in range(count):
            ind = random.randrange(len(nodes))
            node = nodes.pop(ind)
            # prevent node 8090 from being stopped as there is not load balancer for other ports
            node = nodes.pop(ind +1) if node == 8090 else node
            self.step(f'stopping node {node} to test recovery')
            self.docker_stop(node)
            stoppedNodes.append(node)
            expectedCount = len(self.nodes) - len(stoppedNodes)
            
            start, timeout = time.time(), 60
            while time.time() - start < timeout:
                quorum, rc = self.probe('/cluster/pyql/ready')
                quorumCount = len(quorum['nodes']['nodes']) if isinstance(quorum['nodes']['nodes'], list) else 0
                if rc == 200 and quorumCount == expectedCount:
                    break
                time.sleep(5)
                print(f"quorum count {quorumCount} - expected {expectedCount}")
            try:
                assert quorumCount == expectedCount, f"expected number of nodes to be {expectedCount} after stopping node {node} - found {quorumCount} in {quorum}"
            except Exception as e:
                assert False, f"{repr(e)} - error comparing {quorumCount} and {expectedCount}"
        # re-enable node
        for node in stoppedNodes:
            self.step(f'restarting stopped node {node} to test recovery')
            self.docker_restart(node)
        self.sync_job_check()
    def insync_and_state_check(self):
        """
        checks state of tables & querries sync_job_check until state is inSync True
        """
        self.step('verifying tables are properly synced on all endpoints')
        isOk = True
        limit, count = 10, 0
        while count < limit:
            try:
                stateCheck, rc = self.probe('/cluster/pyql/table/state/select')
                assert rc == 200, f"something wrong happened when checking state table {rc}"
                for state in stateCheck['data']:
                    if not state['inSync'] == True or not state['state'] == 'loaded':
                        print(f"found state which was not inSync=True & 'loaded {state}, retrying")
                        isOk = False
                        self.sync_job_check()
                        break
                if isOk:
                    break
                count+=1
            except Exception as e:
                print(f"something wrong happened when checking state table")
                break     

def get_auth_http_headers(method, auth):
    headers = {'Accept': 'application/json', "Content-Type": "application/json"}
    if method == 'token':
        headers['Authentication'] = f'Token {auth}'
    else:
        headers['Authentication'] = f'Basic {auth}'
    return headers
def probe(path, method='GET', data=None, timeout=20.0, auth=None, **kw):
    action = requests if not 'session' in kw else kw['session']
    if 'method' in auth and 'auth' in auth:
        headers = get_auth_http_headers(**auth)
    try:
        if method == 'GET':
            r = action.get(f'{path}', headers=headers, timeout=timeout)
        else:
            r = action.post(f'{path}', headers=headers, data=json.dumps(data), timeout=timeout)
    except Exception as e:
        error = f"probe - Encountered exception when probing {path} - {repr(e)}"
        return error, 500
    try:
        return r.json(),r.status_code
    except:
        return r.text, r.status_code

testCluster = cluster(debug=True)

def test_expand_cluster(count):
    """
    count - number of nodes to expand cluster by 
    """
    joinToken, rc = testCluster.probe(
        f'/auth/token/join',
        auth={
            'method': 'basic', 'auth': testCluster.config['initAdminPw']
        }
    )
    testCluster.step("test_03_cluster_expansion - expanding cluster to test resync mechanisms & expandability")
    for _ in range(count):
        testCluster.expand_cluster(joinToken['join'])

    testCluster.step('wait 15 seconds and begin probing for "type": "syncjobs" jobs in jobs queue which are syncing newly added node')
    time.sleep(10)
    sync_job_check()

def sync_job_check():
    # checking for sync jobs
    maxCheck = 240 # should take less than 60 seconds for new sync jobs 
    start = time.time()
    while time.time() - start < maxCheck:
        jobs, rc = testCluster.get_cluster_jobs()
        if rc == 200:
            jobs = [ job['type'] for job in jobs['data'] ]
        if 'syncjobs' in jobs:
            break
        print(f"waiting for sync jobs to start {jobs} - {time.time() - start:.2f}")
        time.sleep(5)
    assert 'syncjobs' in jobs, f"should take less than {maxCheck} seconds for new sync jobs"

    testCluster.step('syncjobs detected, waiting for pyql tables to sync')
    maxSyncRunTimePerJob = 45
    startTime = time.time()
    lastCount = len(testCluster.get_cluster_jobs('syncjobs')[0]['data'])
    while time.time() - startTime < maxSyncRunTimePerJob:
        jobs, rc = testCluster.get_cluster_jobs('syncjobs')
        if rc == 200:
            if len(jobs['data']) < lastCount or len(jobs['data']) > lastCount:
                lastCount = len(jobs['data'])
                startTime = time.time()
            if lastCount == 0:
                break
        print(f"waiting for {lastCount} sync jobs to complete {time.time() - startTime} sec")
        time.sleep(5)
    assert lastCount == 0, f"waited too long on a syncjobs job to finish, {jobs}"


class PyqlCluster(unittest.TestCase):
    #1 - Test Client Authentication & Token Generation
    def test_00_init_cluster(self):
        testCluster.step('test_init_cluster')
        testCluster.init_cluster()

    def test_01_auth(self):
        # Test Basic auth by pulling token from /auth/token/cluster
        testCluster.auth_setup()

    def test_02_token(self):
        # Using token - pull join token to be used by pyql cluster expansion /auth/token/join
        testCluster.step('test_token Using token to verify jobs in job queue')
        # Using token - verfy cron jobs are correctly added
        maxWait = 120
        start = time.time()
        while time.time() - start < maxWait:
            jobs, rc = testCluster.get_cluster_jobs()
            if rc == 200:
                jobs = [ job['name'] for job in jobs['data'] ]
                if 'tablesync_check' in jobs and 'clusterJob_cleanup' in jobs:
                    break
            time.sleep(10)
        for i in [30, 90]:
            for job in ['tablesync_check', 'clusterJob_cleanup']:
                assert f'{job}_{i}' in jobs, f'{job}_{i} cron job is missing after cluster init - jobs {jobs}'
    #Cluster Expansion testing
    def test_03_cluster_expansion(self): 
        testCluster.expand_cluster()
        testCluster.sync_job_check()
        testCluster.verify_data()
    def test_04_muliti_cluster_expansion(self): 
        for _ in range(4):
            testCluster.expand_cluster()
            testCluster.sync_job_check()
        testCluster.verify_data()
    
    # Cluster Recovery 
    def test_05_cluster_recovery(self):
        testCluster.step(f"test_05_cluster_recovery - bring down random node to verify node removal & recovery upon restarting")
        node = testCluster.nodes[random.randrange(len(testCluster.nodes)) - 1]
        # prevent node 8090 from being stopped as there is not load balancer for other ports
        node = node + 1 if node == 8090 else node
        testCluster.docker_stop(node)
        
        start, timeout = time.time(), 60
        while time.time() - start < timeout:
            quorum, rc = testCluster.probe('/cluster/pyql/ready')
            if rc == 200 and len(quorum['nodes']['nodes']) == len(testCluster.nodes) - 1:
                break
            time.sleep(5)
        try:
            assert len(quorum['nodes']['nodes']) == len(testCluster.nodes) - 1, f"expected number of nodes to be {len(quorum['nodes']['nodes'])} after stopping node {node} - found {quorum}"
        except Exception as e:
            assert False, f"could not check length on {quorum}"

        # re-enable node
        testCluster.step('test_05_cluster_recovery - restarting stopped node to test recovery')
        testCluster.docker_restart(node)
        testCluster.sync_job_check()

    # Verify successful sync
    def test_06_verify_cluster_sync(self):
        testCluster.step('test_06_verify_cluster_sync verifying tables are properly synced on all endpoints')
        maxTries = 3
        isOk = True
        for _ in range(maxTries):
            stateCheck, rc = testCluster.probe('/cluster/pyql/table/state/select')
            assert rc == 200, f"something wrong happened when checking state table {rc}"
            for state in stateCheck['data']:
                if not state['inSync'] == True or not state['state'] == 'loaded':
                    print(f"found state which was not inSync=True & 'loaded {state}, retrying")
                    isOk = False
                    self.sync_job_check()
                    break
            if isOk:
                break
        for state in stateCheck['data']:
            assert state['inSync'] == True and state['state'] == f'loaded', f"found state which was not inSync=True & 'loaded {state}"
        
        # check each endpoint individually to verify if there are any differences in state
        testCluster.verify_data()

    # Cluster Recovery 
    def test_07_multi_cluster_recovery(self):
        testCluster.test_node_recovery(2)

    def test_08_multi_cluster_expand_recovery(self):
        testCluster.step("starting test_08_multi_cluster_expand_recovery - expand")
        # expand by 4, 2 at a time beween sync job checks
        for _ in range(2):
            for _ in range(2):
                testCluster.expand_cluster()
                testCluster.sync_job_check()
            testCluster.verify_data()
        testCluster.step("starting test_08_multi_cluster_expand_recovery - recovery(break/heal)")
        testCluster.test_node_recovery(3)
