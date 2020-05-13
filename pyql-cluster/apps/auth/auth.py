# auth
def run(server):
    from flask import request, make_response
    import os, uuid, time, json, base64, jwt, string, random, socket
    hostname = socket.getfqdn()
    charNums = string.ascii_letters + ''.join([str(i) for i in range(10)])
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
        userSel = server.clusters.auth.select('id', 'password', where={'username': user})
        if len(userSel) > 0:
            log.warning(f"checking auth for {userSel}")
            try:
                decoded = decode_password(userSel[0]['password'], pw)
                return {"message": f"Auth Ok", "userid": userSel[0]['id']}, 200
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
                    tokenType = 'PYQL_CLUSTER_TOKEN_KEY' if not location == 'local' else 'PYQL_LOCAL_TOKEN_KEY'
                    key = server.env[tokenType]
                    log.warning(f"checking auth from {check_auth.__name__} for {f} {args} {kwargs} {request.headers}")
                    if not 'Authentication' in request.headers:
                        return {"error": debug(log.error("missing Authentication"))}, 401
                    auth = request.headers['Authentication']
                    if 'Token' in auth:
                        token = auth.split(' ')[1].rstrip()
                        #decodedToken = decode(token, os.environ[tokenType])
                        decodedToken = decode(token, key)
                        if decodedToken == None:
                            return {"error": debug(log.error(f"token authentication failed"))}, 401
                        request.auth = decodedToken['id']
                        if 'join' in decodedToken['expiration']:
                            # Join tokens should only be used to join an endpoint to a cluster
                            if not 'join_cluster' in str(f):
                                error = log.error(f"token authentication failed, join token auth attempted for {f}")
                                return {"error": debug(error)}, 401
                        if isinstance(decodedToken['expiration'], float):
                            if not decodedToken['expiration'] > time.time():
                                warning = f"token valid but expired for user with id {decodedToken['id']}"
                                return {"error": debug(log.warining(warning))}, 401 #TODO - Check returncode for token expiration
                        log.warning(f"token auth successful for {request.auth} using type {tokenType} key {key}")
                    if 'Basic' in auth:
                        base64Cred = auth.split(' ')[1]
                        creds = base64.decodestring(base64Cred.encode('utf-8')).decode()
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
                        childUsers = server.clusters.auth.select('id', where={'parent': request.auth})
                        request.authChildren = [user['id'] for user in childUsers]
                    if location == 'pyql':
                        pyql, rc = server.get_clusterid_by_name_authorized('pyql')
                        if not rc == 200:
                            log.warning(pyql)
                            return {"error": debug(log.error("un-authorized access"))}, 403
                return f(*args, **kwargs)
            check_auth.__name__ = '_'.join(str(uuid.uuid4()).split('-'))
            return check_auth
        return is_auth
    server.is_authenticated = is_authenticated

    @server.route('/auth/setup/<location>', methods=['POST'])
    @server.is_authenticated('local')
    def cluster_set_service_token(location):
        """
        used primary to update joining nodes with a PYQL_CLUSTER_SERVICE_TOKEN 
        so joining node can pull and set its PYQL_CLUSTER_TOKEN_KEY
        """
        if location == 'cluster':
            serviceToken = request.get_json()
            if not 'PYQL_CLUSTER_SERVICE_TOKEN' in serviceToken:
                return {"error": log.error(f"missing PYQL_CLUSTER_SERVICE_TOKEN")}, 400
            server.env['PYQL_CLUSTER_SERVICE_TOKEN'] = serviceToken['PYQL_CLUSTER_SERVICE_TOKEN']
            r, rc = server.probe(
                f"http://{os.environ['PYQL_CLUSTER_SVC']}/auth/key/cluster",
                auth='remote',
                token=server.env['PYQL_CLUSTER_SERVICE_TOKEN'],
                session=server.get_endpoint_sessions(server.env['PYQL_ENDPOINT'])
            )
            if not 'PYQL_CLUSTER_TOKEN_KEY' in r:
                warning = f"error pulling key {r} {rc}"
                return {"error": log.error(warning)}, rc
            setKey, rc = set_token_key('cluster', r)
            if not rc == 200:
                log.warning(setKey)
            return setKey, rc


    @server.route('/auth/key/<location>', methods=['POST'])
    @server.is_authenticated('local')
    def cluster_set_token_key(location):
        return set_token_key(location)
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


    #if not 'PYQL_LOCAL_TOKEN_KEY' in os.environ:
    if not 'PYQL_LOCAL_TOKEN_KEY' in server.env:
        log.warning('creating PYQL_LOCAL_TOKEN_KEY')
        r, rc = set_token_key(  
            'local', 
            {'PYQL_LOCAL_TOKEN_KEY': ''.join(random.choice(charNums) for i in range(12))}
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
                {'PYQL_CLUSTER_TOKEN_KEY': ''.join(random.choice(charNums) for i in range(24))}
                )

    @server.route('/auth/<authtype>/register', methods=['POST'])
    @server.is_authenticated('pyql')
    def auth_user_register(authtype):
        return user_register(authtype)
    def user_register(authtype, userInfo=None):
        pyql = server.env['PYQL_UUID']
        userInfo = request.get_json() if userInfo == None else userInfo
        userInfo['type'] = authtype
        requiredFields = set()
         # 3 types 'user', 'admin', 'service'
        if authtype == 'user' or authtype == 'admin':
            requiredFields = {'username', 'password', 'email'}
        for field in requiredFields:
            if not field in userInfo:
                return {"error": f"missing {field}"}, 400
        if 'email' in userInfo:
            if not '@' in userInfo['email']:
                return {"error": f"enter a valid email"}, 400
            atIndex = userInfo['email'].index('@')
            if not '.' in userInfo['email'][atIndex:]:
                return {"error": f"enter a valid email"}, 400 
            emailCheck, rc = server.cluster_table_select(
                pyql, 'auth', 
                method='POST', 
                data={
                    'select': ['id', 'password'], 
                    'where': {'email': userInfo['email']}
                }
            )
            if len(emailCheck['data']) > 0:
                return {"error": f"an account with provided email already exists {emailCheck}"}, 400
            if not len(userInfo['password']) >= 8:
                return {"error", f"password must be a atleast 8 chars"}, 400
            userInfo['password'] = encode_password(userInfo['password'])
        # User is unique - adding user
        userInfo['id'] = str(uuid.uuid1())
        log.warning(f"creating new user with id {userInfo['id']}")
        response, rc = server.cluster_table_insert(pyql, 'auth', userInfo)
        if rc == 200: # TODO - should this be 201?
            if authtype == 'user' or authtype == 'admin':
                svc, status = user_register('service', {'parent': userInfo['id']})
                log.warning(f"creating service account for new user {svc} {status}")
            return {"message": f"user created successfully"}, 200
        return response, rc
        
    def create_auth_token(userid, expiration, location):
        secret = server.env[f'PYQL_{location.upper()}_TOKEN_KEY']
        data = {'id': userid, 'expiration': expiration}
        if expiration == 'join':
            data['createTime'] = time.time()
        token = encode(secret, **data)
        log.warning(f"create_auth_token created token {token} using {secret} from {location}")
        return token
    server.create_auth_token = create_auth_token
    
    @server.route('/auth/user/token', methods=['POST'])
    @server.is_authenticated('cluster')
    def auth_token():
        token = create_auth_token(request.auth, time.time()+3600.0, 'cluster')
        if not token == None:
            return {'token': token}, 200
        return {"error": log.error("server error creating user token")}, 500

    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        if not 'PYQL_CLUSTER_SERVICE_TOKEN' in server.env:
            #initializing cluster admin user
            adminId = str(uuid.uuid1())
            log.warning(f'creating admin user with id {adminId}')
            server.data['cluster'].tables['auth'].insert(
                **{
                    'id': adminId,
                    'username': 'admin',
                    'type': 'admin',
                    'password': encode_password(os.environ['PYQL_CLUSTER_INIT_ADMIN_PW']) # init pw
                }
            )
            # initializing cluster service user 
            # cluster service token is used for expanding pyql cluster or other joining endpoints
            clusterServiceId = str(uuid.uuid1())
            server.data['cluster'].tables['auth'].insert(**{
                'id': clusterServiceId,
                'username': 'pyql',
                'type': 'service',
                'parent': adminId
            })
            clusterServiceToken = create_auth_token(clusterServiceId, 'never', 'CLUSTER')
            server.env['PYQL_CLUSTER_SERVICE_TOKEN'] = str(clusterServiceToken)
            log.warning(f"PYQL_CLUSTER_SERVICE_TOKEN set to {server.env['PYQL_CLUSTER_SERVICE_TOKEN']}")
            #os.environ['PYQL_CLUSTER_SERVICE_TOKEN'] = str(clusterServiceToken)
            #NOTE PYQL_CLUSTER_SERVICE_KEY should be input as an env var for non-init endpoints

        
    # check for existing local pyql service user, create if not exists
    pyqlServiceUser = server.data['cluster'].tables['authlocal'].select('*', where={'username': 'pyql'})

    if not len(pyqlServiceUser) > 0:
        serviceId = str(uuid.uuid1())
        server.data['cluster'].tables['authlocal'].insert(**{
            'id': serviceId,
            'username': 'pyql',
            'type': 'service'
        })
        log.warning(f"created new service account with id {serviceId}")
        serviceToken = create_auth_token(serviceId, 'never', 'LOCAL')
        log.warning(f"created service account token {serviceToken}")
    else:
        log.warning(f"found existing service account")
        serviceToken = create_auth_token(
            pyqlServiceUser[0]['id'], 
            'never', 'LOCAL')
    # Local Token
    server.env['PYQL_LOCAL_SERVICE_TOKEN'] = serviceToken


    @server.route('/auth/tokenkey/update', methods=['POST'])
    @server.is_authenticated('local')
    def cluster_key_update(key=None):
        key = request.get_json() if not key == None else key
        if not 'key' in key:
            return {"error": f"""missing key in input {key} expected  {"{key': 'keyvalue....'}"}"""}, 400
        if keytype == 'cluster':
            server.env['PYQL_CLUSTER_TOKEN_KEY'] = key['key']
        
    # Retrieve current local / cluster token - requires auth 
    @server.route('/auth/token/<tokentype>')
    @server.is_authenticated('pyql')
    def cluster_service_token(tokentype):
        if tokentype == 'cluster':
            return {"PYQL_CLUSTER_SERVICE_TOKEN": server.env['PYQL_CLUSTER_SERVICE_TOKEN']}, 200
        if tokentype == 'local':
            return {"PYQL_LOCAL_SERVICE_TOKEN": server.env['PYQL_LOCAL_SERVICE_TOKEN']}, 200

    # Retrieve current local / cluster token - requires auth 
    @server.route('/auth/token/join')
    @server.is_authenticated('cluster')
    def cluster_service_join_token():
        serviceId, rc = server.cluster_table_select(
            server.env['PYQL_UUID'],
            'auth', 
            data={
                'select': '*', 
                'where': {'parent': request.auth, 'type': 'service'}
                },
            method='POST'
        )
        log.warning(f"join token creating for - {serviceId}")
        if len(serviceId['data']) > 0:
            return {"join": create_auth_token(serviceId['data'][0]['id'], 'join', 'CLUSTER')}
        return {"error": log.error(f"unable to find a service account for user")}, 400

    # Retrieve current local / cluster token keys - requires auth 
    @server.route('/auth/key/<keytype>')
    @server.is_authenticated('cluster')
    def cluster_service_token_key(keytype):
        if keytype == 'cluster':
            return {"PYQL_CLUSTER_TOKEN_KEY": server.env['PYQL_CLUSTER_TOKEN_KEY']}, 200
        if keytype == 'local':
            return {"PYQL_LOCAL_TOKEN_KEY": server.env['PYQL_LOCAL_TOKEN_KEY']}, 200
        return {"error": log.error(f"invalid token type specified {tokentype} - use cluster/local")}, 400