import os, unittest, json, requests, time, random

def pad(length, value, char=' '):
    # Account for start & end '#'
    length = length - 4
    padding = length - len(str(value))
    if padding % 2 == 0:
        p1, p2 = int(padding / 2), int(padding / 2)
    else:
        p1, p2 = int((padding + 1) / 2), int((padding -1) / 2)
    p1_air = ''.join(char for _ in range(p1))
    p2_air = ''.join(char for _ in range(p2))

    return f"#{p1_air} {value} {p2_air}#"


class cluster:
    def __init__(self, **kw):
        self.steps = 0
        self.session = requests.Session()
        # loads 
        # {'cluster_port': 8090, 'cluster_ip': '192.168.3.33', 'init_admin_pw': 'YWRtaW46YWJjZDEyMzQ='}
        self.nodes = []
        if not 'config' in kw:
            with open('pyql-cluster/test_config.json', 'r') as config:
                self.config = json.loads(config.read())
        else:
            self.config = config
        self.debug = True if 'debug' in kw else False
    def step(self, action):
        print(f'starting step {self.steps} - {action}')
        self.steps+=1
    def auth_setup(self):
        # Test Basic auth by pulling token from /auth/token/cluster
        self.step('test_auth - waiting for cluster to finish initalizing')
        self.step('test_auth - trying to pull cluster_token')
        for _ in range(3):
            time.sleep(10)
            token, rc = self.probe(
                f'/auth/token/cluster',
                auth={
                    'method': 'basic', 'auth': self.config['init_admin_pw']
                }
            )
            if rc == 200:
                break
        print(f"token {token} rc {rc}")
        self.cluster_token = token['PYQL_CLUSTER_SERVICE_TOKEN']
        print(f"token pulled {self.cluster_token}")
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
        self.nodes = [self.config['cluster_port']]
        host = self.config['cluster_ip']
        port = self.config['cluster_port']
        debug = '--debug' if self.debug == True else ''
        os.system(f"./restart_pyql_cluster.sh dryrun.0.0 {port} {host} {port} init --no-cache {debug}")
    def expand_cluster(self, token=None, port=None, join='join'):
        if token == None:
            token, rc = self.probe(
                f'/auth/token/join',
                auth={
                    'method': 'basic', 'auth': self.config['init_admin_pw']
                }
            )
            assert isinstance(token, dict), f"expected token {token} is type dict , found {type(token)} with value {token}"
            token = token['join']
        if port == None:
            self.nodes.append(self.nodes[-1]+1)
            port = self.nodes[-1]
            print(f"expanding cluster using port {port}")
        cluster_host = self.config['cluster_ip']
        cluster_port = self.config['cluster_port']
        debug = '--debug' if self.debug == True else ''
        
        os.system(f"./restart_pyql_cluster.sh dryrun.0.0 {port} {cluster_host} {cluster_port} {join} {token} '' {debug}")
    def verify_data(self, try_count=0):
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
            diff = []
            #print(f"tables {tables}")
            for tb in tables['data']:
                table = tb['name']
                if table == 'jobs':
                    continue
                data_to_verify = {}
                config = None

                table_endpoints, rc = self.probe(f"/cluster/{cluster['id']}/table/{table}/endpoints")

                for endpoint in table_endpoints['loaded']:
                    endpoint_info = table_endpoints['loaded'][endpoint]

                # pull config from table to verify primary_key to sort data on

                    if config == None:
                        config, rc = probe(
                                f"http://{endpoint_info['path']}/db/{endpoint_info['db_name']}/table/{table}/config",
                                auth={  
                                    'method': 'token',
                                    'auth': endpoint_info['token']
                                },
                                session=self.session
                            )
                        config = config[table]
                        print(f"pulled config: {config} with primary key: {config['primary_key']}")

                    data_to_verify[endpoint], rc = probe(
                        f"http://{endpoint_info['path']}/db/{endpoint_info['db_name']}/table/{table}/select",
                        auth={  
                            'method': 'token',
                            'auth': endpoint_info['token']
                        },
                        session=self.session
                    )
                    if rc == 200:
                        data_to_verify[endpoint] = data_to_verify[endpoint]['data']
                        #print(f"data_to_verify {data_to_verify[endpoint]}")
                    else:
                        assert False, f"data_to_verify ERROR  when acessing endpoint: {endpoint_info} table: {table} -- {data_to_verify[endpoint]}"
                verify[table] = {}
                for endpoint in data_to_verify:
                    verify[table][endpoint] = {'status': [], 'diff': {}}
                    for ep in data_to_verify:
                        if ep == endpoint:
                            continue
                        #txns = sorted(txns, key=lambda txn: txn['timestamp'])
                        for t1r, t2r, in zip(
                            sorted(
                                data_to_verify[endpoint],
                                key=lambda row: row[config['primary_key']]
                                ),
                            sorted(
                                data_to_verify[ep],
                                key=lambda row: row[config['primary_key']]
                                )
                            ):
                            if not t1r == t2r:
                                if not ep in verify[table][endpoint]['diff']:
                                    verify[table][endpoint]['diff'][ep] = []
                                t_keys = list(t1r.keys())
                                
                                row_len_keys = len(str(t1r.keys())) + 4*len(t_keys)
                                row_len_values = len(str(t1r.values())) + 4*len(t_keys)
                                row_len = row_len_values if row_len_values >= row_len_keys else row_len_keys

                                diff.append(''.join([' ' for _ in range(row_len)]))
                                diff.append(''.join(['#' for _ in range(row_len)]))
                                diff.append(pad(row_len, f"DIFF {endpoint} - Table: {table}"))
                                diff.append(pad(row_len, f"Checking {ep} - Table: {table}"))
                                diff.append(''.join(['#' for _ in range(row_len)]))
                                t_key_len = {k: len(str(k)) for k in t_keys}
                                t_val_len = {k: len(str(t1r[k])) for k in t_keys}
                                t_len = {}
                                for tk, tv in zip(t_key_len.items(), t_val_len.items()):
                                    t_len[tk[0]] = tk[1] if tk[1] >= tv[1] else tv[1]
                                
                                header = ''
                                for k in t_keys:
                                    header = f"{header}{pad(t_len[k], k)}"
                                diff.append(header)
                                diff.append(''.join(['_' for _ in range(row_len)]))
                                t_row1 = ''
                                t_row2 = ''
                                for col_t1, col_t2 in zip(t1r.items(), t2r.items()):
                                    t_row1 = f'{t_row1}{pad(t_len[col_t1[0]], col_t1[1])}'
                                    t_row2 = f'{t_row2}{pad(t_len[col_t1[0]], col_t2[1])}'
                                diff.append(t_row1)
                                diff.append(t_row2)

                                verify[table][endpoint]['diff'][ep].append((t1r, t2r))
                                # every subsequent row will not be equal from here
                                break
                        if ep in verify[table][endpoint]['diff']:
                            verify[table][endpoint]['status'].append(False)
                            continue
                        verify[table][endpoint]['status'].append(True)
            
            verify_fail = []
            diff_tables = []
            for table, endpoints in verify.items():
                for endpoint, status in endpoints.items():
                    if table == 'jobs':
                        continue
                    if 'txn' in table and 'jobs' in table:
                        continue
                    if False in status['status']: 
                        if try_count < 2:
                            print(f"{table} endpoint {endpoint} data did not match with {status['diff'].keys()} - retrying")
                            time.sleep(5)
                            self.verify_data(try_count+1)
                            return # avoid asserting if this is not the max retry run
                    if False in status['status']:
                        verify_fail.append(f"{table} endpoint {endpoint} data did not match with {status['diff']}")
            if not len(verify_fail) == 0:
                print(verify_fail)
                print('\n'.join(diff))
            assert len(verify_fail) == 0, f"verification failed on endpoint(s)"
            print(f"verify completed for cluster {cluster['name']} - {verify}")
            #assert not False in status['status'], f"{table} endpoint {endpoint} data did not match with {status['diff']}"


    def get_cluster_jobs(self, job_type=None):
        if job_type == None:
            return self.probe(
                '/cluster/pyql/table/jobs/select', 
                auth={'method': 'token', 'auth': self.cluster_token})
        return self.probe(
            '/cluster/pyql/table/jobs/select', method='POST',
            data={
                'select': ['*'], 'where': {
                    'type': job_type
                }
            },
            auth={'method': 'token', 'auth': self.cluster_token})
    def probe(self, path, **kw):
        cluster_ip = self.config['cluster_ip']
        cluster_port = self.config['cluster_port']
        kw['auth'] = {'method': 'token', 'auth': self.cluster_token} if not 'auth' in kw else kw['auth']
        kw['session'] = self.session
        return probe(f"http://{cluster_ip}:{cluster_port}{path}", **kw)
    def sync_job_check(self):
        # checking for sync jobs
        max_check = 240 # should take less than 60 seconds for new sync jobs 
        start = time.time()
        while time.time() - start < max_check:
            jobs, rc = self.get_cluster_jobs()
            if rc == 200:
                jobs = [ job['type'] for job in jobs['data'] ]
            if 'syncjobs' in jobs:
                break
            print(f"waiting for sync jobs to start {jobs} - {time.time() - start:.2f}")
            time.sleep(5)
        assert 'syncjobs' in jobs, f"should take less than {max_check} seconds for new sync jobs"

        self.step('syncjobs detected, waiting for pyql tables to sync')
        time.sleep(10)
        MAX_SYNC_RUN_TIME_PER_JOB = 300
        start_time = time.time()
        last_count = len(self.get_cluster_jobs('syncjobs')[0]['data'])
        last_job = None
        while time.time() - start_time < MAX_SYNC_RUN_TIME_PER_JOB:
            jobs, rc = self.get_cluster_jobs('syncjobs')
            if rc == 200:
                if len(jobs['data']) < last_count or len(jobs['data']) > last_count:
                    last_count = len(jobs['data'])
                    start_time = time.time()
                if len(jobs['data']) == 1:
                    if last_job == jobs['data'][0]['id']:
                        continue
                    else:
                        last_job = jobs['data'][0]['id']
                        last_count = len(jobs['data'])
                        start_time = time.time()
                if last_count == 0:
                    break
            print(f"waiting for {last_count} sync jobs to complete {time.time() - start_time:.2f} sec")
            time.sleep(5)
        assert last_count == 0, f"waited too long on a syncjobs job to finish - {time.time() - start_time:.2f}, {jobs}"
    def test_node_recovery(self, count):
        self.step(f"bring down {count} random nodes to verify node removal & recovery upon restarting")
        stopped_nodes = []
        nodes = [n for n in self.nodes]
        for i in range(count):
            ind = random.randrange(len(nodes))
            node = nodes.pop(ind)
            # prevent node 8090 from being stopped as there is not load balancer for other ports
            node = nodes.pop(ind +1) if node == 8090 else node
            self.step(f'stopping node {node} to test recovery')
            self.docker_stop(node)
            stopped_nodes.append(node)
            expected_count = len(self.nodes) - len(stopped_nodes)
            
            start, timeout = time.time(), 60
            while time.time() - start < timeout:
                quorum, rc = self.probe('/cluster/pyql/ready')
                quorum_count = len(quorum['nodes']) if isinstance(quorum['nodes'], dict) else 0
                if rc == 200 and quorum_count == expected_count:
                    break
                time.sleep(5)
                print(f"quorum count {quorum_count} - expected {expected_count}")
            try:
                assert quorum_count == expected_count, f"expected number of nodes to be {expected_count} after stopping node {node} - found {quorum_count} in {quorum}"
            except Exception as e:
                assert False, f"{repr(e)} - error comparing {quorum_count} and {expected_count}"
        # re-enable node
        self.step(f"sleeping 90 seconds before recovery")
        time.sleep(90)
        for node in stopped_nodes:
            self.step(f'restarting stopped node {node} to test recovery')
            self.docker_restart(node)
        self.sync_job_check()
        self.state_check()
        self.verify_data()
    def state_check(self):
        """
        checks state of tables & querries sync_job_check until state is loaded
        """
        self.step('verifying tables are properly synced on all endpoints')
        is_ok = True
        limit, count = 10, 0
        while count < limit:
            try:
                state_check, rc = self.probe('/cluster/pyql/table/state/select')
                assert rc == 200, f"something wrong happened when checking state table {rc}"
                for state in state_check['data']:
                    if not state['state'] == 'loaded':
                        print(f"found state which was not 'loaded {state}, retrying")
                        is_ok = False
                        self.sync_job_check()
                        break
                if is_ok:
                    break
                count+=1
            except Exception as e:
                print(f"something wrong happened when checking state table")
                break
        self.step("all endpoints verfied loaded") 

def get_auth_http_headers(method, auth):
    headers = {'Accept': 'application/json', "Content-Type": "application/json"}
    if method == 'token':
        headers['Authentication'] = f'Token {auth}'
    else:
        headers['Authentication'] = f'Basic {auth}'
    return headers
def probe(path, method='GET', data=None, timeout=60.0, auth=None, **kw):
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
#1 - Test Client Authentication & Token Generation
try:
    test_cluster = cluster(debug=True)
except Exception as e:
    print("Error loading PyqlCluster for unittest")

class PyqlCluster(unittest.TestCase):

    def test_00_init_cluster(self):
        test_cluster.step('test_init_cluster')
        test_cluster.init_cluster()
        test_cluster.auth_setup()

    def test_01_expand(self):
        # Test Basic auth by pulling token from /auth/token/cluster
        for _ in range(2):
            test_cluster.expand_cluster()
        test_cluster.sync_job_check()
        test_cluster.verify_data()
        

    def test_02_token(self):
        # Using token - pull join token to be used by pyql cluster expansion /auth/token/join
        test_cluster.step('test_token Using token to verify jobs in job queue')
        # Using token - verfy cron jobs are correctly added
        MAX_WAIT = 120
        start = time.time()
        while time.time() - start < MAX_WAIT:
            jobs, rc = test_cluster.get_cluster_jobs()
            if rc == 200:
                jobs = [ job['name'] for job in jobs['data'] ]
                missing = False
                for i in [30, 90]:
                    for job in ['tablesync_check', 'cluster_job_cleanup']:
                        if not f'{job}_{i}' in jobs:
                            missing = True
                            break
                if missing:
                    print(jobs)
                    time.sleep(10)
                else:
                    break
        for i in [30, 90]:
            for job in ['tablesync_check', 'cluster_job_cleanup']:
                assert f'{job}_{i}' in jobs, f'{job}_{i} cron job is missing after cluster init - jobs {jobs}'
    #Cluster Expansion testing
    def test_03_muliti_cluster_expansion(self): 
        for _ in range(4):
            test_cluster.expand_cluster()
        test_cluster.sync_job_check()
        test_cluster.verify_data()
    
    # Cluster Recovery 
    def test_04_cluster_recovery(self):
        count = 2
        test_cluster.step(f"test_05_cluster_recovery - bring down {count} random node(s) to verify node removal & recovery upon restarting")
        test_cluster.test_node_recovery(2)
        test_cluster.state_check()
        test_cluster.verify_data()

    def test_05_multi_cluster_expand_recovery(self):
        test_cluster.step("starting test_08_multi_cluster_expand_recovery - expand")
        # expand by 4, 2 at a time beween sync job checks
        for _ in range(4):
            test_cluster.expand_cluster()
        test_cluster.sync_job_check()
        test_cluster.verify_data()
        test_cluster.step("starting test_08_multi_cluster_expand_recovery - recovery(break/heal)")
        test_cluster.test_node_recovery(3)