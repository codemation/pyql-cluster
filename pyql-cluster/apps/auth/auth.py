# auth
def run(server):
    from flask import request, make_response
    import os, uuid, time, json, base64, jwt, string, random, socket
    hostname = socket.getfqdn()
    char_nums = string.ascii_letters + ''.join([str(i) for i in range(10)])
    log = server.log
    def debug(error):
        if os.environ.get('PYQL_DEBUG') == True:
            error = f"{error} from pyql host {hostname}"
        return error

    def encode(secret, **kw):
        try:
            return jwt.encode(kw, secret, algorithm='HS256').decode()
        except Exception as e:
            log.exception(f"error encoding {kw} using {secret}")
    server.encode = encode
    def decode(token, secret):
        try:
            return jwt.decode(token.encode('utf-8'), secret, algorithm='HS256')
        except Exception as e:
            log.exception(f"error decoding token {token} using {secret}")

    def encode_password(pw):
        return encode(pw, password=pw, time=time.time())
    def decode_password(encodedPw, auth):
        return decode(encodedPw, auth)
    def validate_user_pw(user, pw):
        user_creds = server.clusters.auth.select('id', 'password', where={'username': user})
        if len(user_creds) > 0:
            log.warning(f"checking auth for {user_creds}")
            try:
                decoded = decode_password(user_creds[0]['password'], pw)
                return {"message": f"Auth Ok", "userid": user_creds[0]['id']}, 200
            except Exception as e:
                log.exception(f"Auth failed for user {user} - invalid credentials")
        return {"message": debug(f"user / pw combination does not exist or is incorrect")}, 401
    def is_authenticated(location):
        """
        usage:
            @server.route('/')
            @is_authenticated('local') Or @is_authenticated('cluster')
            def home_func():
                #Add Code here
                print("Hello home World") 
                return {"message": "Hello home World"}, 200
        """
        def is_auth(f):
            def check_auth(*args, **kwargs):
                if not 'auth' in request.__dict__:
                    token_type = 'PYQL_CLUSTER_TOKEN_KEY' if not location == 'local' else 'PYQL_LOCAL_TOKEN_KEY'
                    key = server.env[token_type]
                    log.warning(f"checking auth from {check_auth.__name__} for {f} {args} {kwargs} {request.headers}")
                    if not 'Authentication' in request.headers:
                        return {"error": debug(log.error("missing Authentication"))}, 401
                    auth = request.headers['Authentication']

                    # Token handling
                    if 'Token' in auth:
                        token = auth.split(' ')[1].rstrip()
                        decoded_token = decode(token, key)
                        if decoded_token == None:
                            return {"error": debug(log.error(f"token authentication failed"))}, 401
                        request.auth = decoded_token['id']
                        if isinstance(decoded_token['expiration'], dict) and 'join' in decoded_token['expiration']:
                            # Join tokens should only be used to join an endpoint to a cluster
                            if not 'join_cluster' in str(f):
                                error = log.error(f"token authentication failed, join token auth attempted for {f}")
                                return {"error": debug(error)}, 401
                        if isinstance(decoded_token['expiration'], float):
                            if not decoded_token['expiration'] > time.time():
                                warning = f"token valid but expired for user with id {decoded_token['id']}"
                                return {"error": debug(log.warning(warning))}, 401
                        log.warning(f"token auth successful for {request.auth} using type {token_type} key {key}")

                    # Basic Authentication Handling
                    if 'Basic' in auth:
                        base64_cred = auth.split(' ')[1]
                        creds = base64.decodestring(base64_cred.encode('utf-8')).decode()
                        if not ':' in creds:
                            return {
                                "error": "Basic authentication did not contain user pw separated by ':' Use: echo user:password | base64"
                                }, 400
                        username, password = creds.split(':')
                        response, rc = validate_user_pw(username, password)
                        if not rc == 200:
                            error = f"auth failed from {check_auth.__name__} for {f} - username {username}"
                            return {"error": debug(log.error(error))}, 401
                        request.auth = response['userid']
                        # check if userid is a parent for other users

                    if location == 'local':
                        if not request.auth in server.clusters.authlocal:
                            return {"error": debug(log.error("un-authorized access"))}, 403
                    else:
                        child_users = server.clusters.auth.select('id', where={'parent': request.auth})
                        request.auth_children = [user['id'] for user in child_users]
                    if location == 'pyql':
                        pyql, rc = server.get_clusterid_by_name_authorized('pyql')
                        if not rc == 200:
                            log.warning(pyql)
                            return {"error": debug(log.error("un-authorized access"))}, 403
                return f(*args, **kwargs)
            # modifies check_auth func name to be unique - required for flask endpoint routing
            check_auth.__name__ = '_'.join(str(uuid.uuid4()).split('-'))
            return check_auth
        return is_auth
    server.is_authenticated = is_authenticated


    def set_token_key(location, value=None):
        """
        expects:
            location = cluster|local
            value = {'PYQL_LOCAL_TOKEN_KEY': 'key....'} | {'PYQL_CLUSTER_TOKEN_KEY': 'key....'}
        """
        if location == 'cluster' or location == 'local':
            key = f'PYQL_{location.upper()}_TOKEN_KEY'
            keydata = request.get_json() if value == None else value
            if key in keydata:
                value = keydata[key]
                server.env[key] = value
                return {"message": log.warning(f"{key} updated successfully with {value}")}, 200
        return {"error": log.error("invalid location or key - specified")}, 400


    if not 'PYQL_LOCAL_TOKEN_KEY' in server.env:
        log.warning('creating PYQL_LOCAL_TOKEN_KEY')
        r, rc = set_token_key(  
            'local', 
            {'PYQL_LOCAL_TOKEN_KEY': ''.join(random.choice(char_nums) for i in range(12))}
            )
        log.warning(f"finished creating PYQL_LOCAL_TOKEN_KEY {server.env['PYQL_LOCAL_TOKEN_KEY']} - {r} {rc}")
    else:
        log.warning(f'PYQL_LOCAL_TOKEN_KEY already existed {server.env["PYQL_LOCAL_TOKEN_KEY"]}')
    
    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        if not 'PYQL_CLUSTER_INIT_ADMIN_PW' in os.environ:
            os.environ['PYQL_CLUSTER_INIT_ADMIN_PW'] = 'abcd1234'
        #os.environ['PYQL_CLUSTER_INIT_ADMIN_PW']
        if not 'PYQL_CLUSTER_TOKEN_KEY' in server.env:
            set_token_key(
                'cluster', 
                {'PYQL_CLUSTER_TOKEN_KEY': ''.join(random.choice(char_nums) for i in range(24))}
                )
        
    def create_auth_token(userid, expiration, location):
        secret = server.env[f'PYQL_{location.upper()}_TOKEN_KEY']
        data = {'id': userid, 'expiration': expiration}
        if expiration == 'join':
            data['create_time'] = time.time()
        token = encode(secret, **data)
        log.warning(f"create_auth_token created token {token} using {secret} from {location}")
        return token
    server.create_auth_token = create_auth_token
    


    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        if not 'PYQL_CLUSTER_SERVICE_TOKEN' in server.env:
            #initializing cluster admin user
            admin_id = str(uuid.uuid1())
            log.warning(f'creating admin user with id {admin_id}')
            server.data['cluster'].tables['auth'].insert(
                **{
                    'id': admin_id,
                    'username': 'admin',
                    'type': 'admin',
                    'password': encode_password(os.environ['PYQL_CLUSTER_INIT_ADMIN_PW']) # init pw
                }
            )
            # initializing cluster service user 
            # cluster service token is used for expanding pyql cluster or other joining endpoints
            cluster_service_id = str(uuid.uuid1())
            server.data['cluster'].tables['auth'].insert(**{
                'id': cluster_service_id,
                'username': 'pyql',
                'type': 'service',
                'parent': admin_id
            })
            cluster_service_token = create_auth_token(cluster_service_id, 'never', 'CLUSTER')
            server.env['PYQL_CLUSTER_SERVICE_TOKEN'] = str(cluster_service_token)
            log.warning(f"PYQL_CLUSTER_SERVICE_TOKEN set to {server.env['PYQL_CLUSTER_SERVICE_TOKEN']}")

        
    # check for existing local pyql service user, create if not exists
    pyql_service_user = server.data['cluster'].tables['authlocal'].select('*', where={'username': 'pyql'})

    if not len(pyql_service_user) > 0:
        service_id = str(uuid.uuid1())
        server.data['cluster'].tables['authlocal'].insert(**{
            'id': service_id,
            'username': 'pyql',
            'type': 'service'
        })
        log.warning(f"created new service account with id {service_id}")
        service_token = create_auth_token(service_id, 'never', 'LOCAL')
        log.warning(f"created service account token {service_token}")
    else:
        log.warning(f"found existing service account")
        service_token = create_auth_token(
            pyql_service_user[0]['id'], 
            'never', 'LOCAL')
    # Local Token
    server.env['PYQL_LOCAL_SERVICE_TOKEN'] = service_token

    @server.route('/auth/key/<location>', methods=['POST'])
    @server.is_authenticated('local')
    def cluster_set_token_key(location):
        return set_token_key(location)

    @server.route('/auth/setup/cluster', methods=['POST'])
    @server.is_authenticated('local')
    def cluster_set_service_token():
        """
        used primary to update joining nodes with a PYQL_CLUSTER_SERVICE_TOKEN 
        so joining node can pull and set its PYQL_CLUSTER_TOKEN_KEY
        """
        service_token = request.get_json()
        if not 'PYQL_CLUSTER_SERVICE_TOKEN' in service_token:
            return {"error": log.error(f"missing PYQL_CLUSTER_SERVICE_TOKEN")}, 400
        server.env['PYQL_CLUSTER_SERVICE_TOKEN'] = service_token['PYQL_CLUSTER_SERVICE_TOKEN']
        r, rc = server.probe(
            f"http://{os.environ['PYQL_CLUSTER_SVC']}/auth/key/cluster",
            auth='remote',
            token=server.env['PYQL_CLUSTER_SERVICE_TOKEN'],
            session=server.session
        )
        if not 'PYQL_CLUSTER_TOKEN_KEY' in r:
            warning = f"error pulling key {r} {rc}"
            return {"error": log.error(warning)}, rc
        set_key, rc = set_token_key('cluster', r)
        if not rc == 200:
            log.warning(set_key)
        return set_key, rc
    # Retrieve current local / cluster token - requires auth 
    @server.route('/auth/token/<tokentype>')
    @server.is_authenticated('pyql')
    def cluster_service_token(tokentype):
        if tokentype == 'cluster':
            return {"PYQL_CLUSTER_SERVICE_TOKEN": server.env['PYQL_CLUSTER_SERVICE_TOKEN']}, 200
        if tokentype == 'local':
            return {"PYQL_LOCAL_SERVICE_TOKEN": server.env['PYQL_LOCAL_SERVICE_TOKEN']}, 200

    # Retrieve current local / cluster token keys - requires auth 
    @server.route('/auth/key/<keytype>')
    @server.is_authenticated('pyql')
    def cluster_service_token_key(keytype):
        if keytype == 'cluster':
            return {"PYQL_CLUSTER_TOKEN_KEY": server.env['PYQL_CLUSTER_TOKEN_KEY']}, 200
        if keytype == 'local':
            return {"PYQL_LOCAL_TOKEN_KEY": server.env['PYQL_LOCAL_TOKEN_KEY']}, 200
        return {"error": log.error(f"invalid token type specified {tokentype} - use cluster/local")}, 400

    def auth_post_cluster_setup(server):
        """
        run following cluster app initializing so that state_and_quorum_check can be run
        """
        @server.trace
        def user_register(authtype, user_info=None, **kw):
            trace = kw['trace']
            pyql = server.env['PYQL_UUID']
            user_info = request.get_json() if user_info == None else user_info
            user_info['type'] = authtype
            required_fields = set()
            # 3 types 'user', 'admin', 'service'
            if authtype == 'user' or authtype == 'admin':
                required_fields = {'username', 'password', 'email'}
            for field in required_fields:
                if not field in user_info:
                    return {"error": f"missing {field}"}, 400
            if 'email' in user_info:
                if not '@' in user_info['email']:
                    return {"error": f"enter a valid email"}, 400
                email_index = user_info['email'].index('@')
                if not '.' in user_info['email'][email_index:]:
                    return {"error": f"enter a valid email"}, 400 
                email_check, rc = server.cluster_table_select(
                    pyql, 'auth', 
                    method='POST', 
                    data={
                        'select': ['id', 'password'], 
                        'where': {'email': user_info['email']}
                    },
                    trace=trace
                )
                if len(email_check['data']) > 0:
                    return {"error": trace(f"an account with provided email already exists {email_check}")}, 400
                if not len(user_info['password']) >= 8:
                    return {"error", trace.error(f"password must be a atleast 8 chars")}, 400
                user_info['password'] = encode_password(user_info['password'])
            # User is unique - adding user
            user_info['id'] = str(uuid.uuid1())
            trace(f"creating new user with id {user_info['id']}")
            response, rc = server.cluster_table_insert(pyql, 'auth', user_info, trace=trace)
            if rc == 200:
                if authtype == 'user' or authtype == 'admin':
                    svc, status = user_register('service', {'parent': user_info['id']})
                    trace(f"creating service account for new user {svc} {status}")
                return {"message": trace(f"user created successfully")}, 201
            return response, rc

        @server.route('/auth/<authtype>/register', methods=['POST'])
        @server.state_and_quorum_check
        @server.is_authenticated('pyql')
        @server.trace
        def auth_user_register(authtype, **kw):
            return user_register(authtype, **kw)

        @server.route('/auth/token/user')
        @server.state_and_quorum_check
        @server.is_authenticated('cluster')
        @server.trace
        def get_user_auth_token(**kw):
            return {"token": create_auth_token(request.auth, time.time() + 3600, 'cluster')}, 200


        # Retrieve current local / cluster token - requires auth 
        @server.route('/auth/token/join')
        @server.state_and_quorum_check
        @server.is_authenticated('cluster')
        @server.trace
        def cluster_service_join_token(**kw):
            trace = kw['trace']
            service_id, rc = server.cluster_table_select(
                server.env['PYQL_UUID'],
                'auth', 
                data={
                    'select': '*', 
                    'where': {'parent': request.auth, 'type': 'service'}
                    },
                method='POST',
                quorum=kw['quorum'],
                trace=trace
            )
            trace(f"join token creating for - {service_id}")
            if len(service_id['data']) > 0:
                return {"join": create_auth_token(service_id['data'][0]['id'], 'join', 'CLUSTER')}
            return {"error": trace.error(f"unable to find a service account for user")}, 400


    server.auth_post_cluster_setup = auth_post_cluster_setup