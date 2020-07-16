# auth
async def run(server):
    from fastapi import Request
    import os, uuid, time, json, base64, jwt, string, random, socket
    import uvloop, asyncio
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
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
    async def validate_user_pw(user, pw):
        user_creds = await server.clusters.auth.select('id', 'password', where={'username': user})
        if len(user_creds) > 0:
            log.warning(f"checking auth for {user_creds}")
            try:
                decoded = decode_password(user_creds[0]['password'], pw)
                return {"message": f"Auth Ok", "userid": user_creds[0]['id']}, 200
            except Exception as e:
                log.exception(f"Auth failed for user {user} - invalid credentials")
        server.http_exception(401, debug(f"user / pw combination does not exist or is incorrect"))
    def is_authenticated(location):
        """
        @server.api_route("/", methods=['POST','GET'])
        async def home_func_endpoint(data: dict, request: Request):
            return await home_func(request)

        @is_authenticated('local')
        async def home_func(request=None, **kw):
            #request = kw['request']
            print("Hello fastpi user World")
            print(request.method)
            if request.method == 'GET':
                return f"<h1>Hello fastapi home World</h1> {kw}", 200
            else:
                data = await request.json()
                return {"message": f"{request.method} updated <h1>Hello fastapi home World</h1>  with {dict(data)}"}
        """
        def is_auth(f):
            async def check_auth(*args, **kwargs):
                request = kwargs['request']
                if not 'authentication' in kwargs:
                    token_type = 'PYQL_CLUSTER_TOKEN_KEY' if not location == 'local' else 'PYQL_LOCAL_TOKEN_KEY'
                    log.warning(f"check_auth - current event loop {server.env.database.loop}")
                    key = await server.env[token_type]
                    log.warning(f"checking auth from {check_auth.__name__} for {f} {args} {kwargs} {request.headers}")
                    if not 'authentication' in request.headers:
                        server.http_exception(401, debug(log.error("missing 'authentication' in headers")))
                    auth = request.headers['authentication']


                    # Token handling
                    if 'Token' in auth:
                        token = auth.split(' ')[1].rstrip()
                        decoded_token = decode(token, key)
                        if decoded_token == None:
                            server.http_exception(401, debug(log.error(f"token authentication failed")))
                        #request.authentication= decoded_token['id']
                        #setattr(request, 'authentication', decoded_token['id'])
                        kwargs['authentication'] = decoded_token['id']
                        request.auth = decoded_token['id']
                        if isinstance(decoded_token['expiration'], dict) and 'join' in decoded_token['expiration']:
                            # Join tokens should only be used to join an endpoint to a cluster
                            if not 'join_cluster' in str(f):
                                error = log.error(f"token authentication failed, join token auth attempted for {f}")
                                server.http_exception(401, debug(error))
                        if isinstance(decoded_token['expiration'], float):
                            if not decoded_token['expiration'] > time.time():
                                warning = f"token valid but expired for user with id {decoded_token['id']}"
                                server.http_exception(401, debug(log.warning(warning)))
                        log.warning(f"token auth successful for {kwargs['authentication']} using type {token_type} key {key}")

                    # Basic Authentication Handling
                    if 'Basic' in auth:
                        base64_cred = auth.split(' ')[1]
                        creds = base64.decodestring(base64_cred.encode('utf-8')).decode()
                        if not ':' in creds:
                            server.http_exception(
                                400,
                                "Basic authentication did not contain user pw separated by ':' Use: echo user:password | base64")
                        username, password = creds.split(':')
                        response, rc = await validate_user_pw(username, password) # TODO - Blocking - need to make async
                        if not rc == 200:
                            error = f"auth failed from {check_auth.__name__} for {f} - username {username}"
                            server.http_exception(401, debug(log.error(error)))
                        kwargs['authentication'] = response['userid']
                        request.auth = response['userid']
                        # check if userid is a parent for other users

                    if location == 'local':
                        if await server.clusters.authlocal[kwargs['authentication']] == None:
                            server.http_exception(403, debug(log.error("un-authorized access")))
                    else:
                        child_users = await server.clusters.auth.select('id', where={'parent': request.auth})
                        log.warning(f"check_auth child_users: {child_users}")
                        request.auth_children = [user['id'] for user in child_users]
                        kwargs['auth_children'] = [user['id'] for user in child_users]
                    if location == 'pyql':
                        pyql, rc = await server.get_clusterid_by_name_authorized('pyql', **kwargs)
                        if not rc == 200:
                            log.warning(pyql)
                            server.http_exception(403, debug(log.error("un-authorized access")))
                return await f(*args, **kwargs)
            # modifies check_auth func name to be unique
            check_auth.__name__ = '_'.join(str(uuid.uuid4()).split('-'))
            return check_auth
        return is_auth
    server.is_authenticated = is_authenticated


    async def set_token_key(location, value):
        """
        expects:
            location = cluster|local
            value = {'PYQL_LOCAL_TOKEN_KEY': 'key....'} | {'PYQL_CLUSTER_TOKEN_KEY': 'key....'}
        """
        if location == 'cluster' or location == 'local':
            key = f'PYQL_{location.upper()}_TOKEN_KEY'
            keydata = value
            if key in keydata:
                value = keydata[key]
                await server.env.set_item(key, value)
                return {"message": log.warning(f"{key} updated successfully with {value}")}
        server.http_exception(400, log.error("invalid location or key - specified"))


    # Create 'PYQL_LOCAL_TOKEN_KEY' if not existent yet.
    PYQL_LOCAL_TOKEN_KEY = await server.env['PYQL_LOCAL_TOKEN_KEY']
    if PYQL_LOCAL_TOKEN_KEY == None:
        log.warning('creating PYQL_LOCAL_TOKEN_KEY')
        r = await set_token_key(  
            'local', 
            {'PYQL_LOCAL_TOKEN_KEY': ''.join(random.choice(char_nums) for i in range(12))}
            )
        log.warning(f"finished creating PYQL_LOCAL_TOKEN_KEY {PYQL_LOCAL_TOKEN_KEY} - {r}")
    else:
        log.warning(f'PYQL_LOCAL_TOKEN_KEY already exists {PYQL_LOCAL_TOKEN_KEY}')
    
    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        if not 'PYQL_CLUSTER_INIT_ADMIN_PW' in os.environ:
            os.environ['PYQL_CLUSTER_INIT_ADMIN_PW'] = 'abcd1234'
        #os.environ['PYQL_CLUSTER_INIT_ADMIN_PW']
        PYQL_CLUSTER_TOKEN_KEY = await server.env['PYQL_CLUSTER_TOKEN_KEY']
        if PYQL_CLUSTER_TOKEN_KEY == None:
            await set_token_key(
                'cluster', 
                {'PYQL_CLUSTER_TOKEN_KEY': ''.join(random.choice(char_nums) for i in range(24))}
                )

    async def create_auth_token(userid, expiration, location):
        secret = await server.env[f'PYQL_{location.upper()}_TOKEN_KEY']
        data = {'id': userid, 'expiration': expiration}
        if expiration == 'join':
            data['create_time'] = time.time()
        token = encode(secret, **data)
        log.warning(f"create_auth_token created token {token} using {secret} from {location}")
        return token
    server.create_auth_token = create_auth_token
    


    if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
        if await server.env['PYQL_CLUSTER_SERVICE_TOKEN'] == None:
            #initializing cluster admin user
            admin_id = str(uuid.uuid1())
            log.warning(f'creating admin user with id {admin_id}')
            await server.data['cluster'].tables['auth'].insert(
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
            await server.data['cluster'].tables['auth'].insert(**{
                'id': cluster_service_id,
                'username': 'pyql',
                'type': 'service',
                'parent': admin_id
            })
            cluster_service_token = await create_auth_token(cluster_service_id, 'never', 'CLUSTER')
            await server.env.set_item('PYQL_CLUSTER_SERVICE_TOKEN', str(cluster_service_token))
            log.warning(f"PYQL_CLUSTER_SERVICE_TOKEN set to {str(cluster_service_token)}")

        
    # check for existing local pyql service user, create if not exists
    pyql_service_user = await server.data['cluster'].tables['authlocal'].select('*', where={'username': 'pyql'})

    if not len(pyql_service_user) > 0:
        service_id = str(uuid.uuid1())
        await server.data['cluster'].tables['authlocal'].insert(**{
            'id': service_id,
            'username': 'pyql',
            'type': 'service'
        })
        log.warning(f"created new service account with id {service_id}")
        service_token = await create_auth_token(service_id, 'never', 'LOCAL')
        log.warning(f"created service account token {service_token}")
    else:
        log.warning(f"found existing service account")
        service_token = await create_auth_token(
            pyql_service_user[0]['id'], 
            'never', 'LOCAL')
    # Local Token
    await server.env.set_item('PYQL_LOCAL_SERVICE_TOKEN', service_token)

    #### Endpoints for setting local & cluster token keys ###

    @server.api_route('/auth/key/local', methods=['POST'])
    async def cluster_set_local_token_key_endpoint(key: dict, request: Request):
        return await cluster_set_token_key('local', key,  request=await server.process_request(request))

    @server.api_route('/auth/key/cluster', methods=['POST'])
    async def cluster_set_cluster_token_key_endpoint(key: dict, request: Request):
        return await cluster_set_token_key('cluster', key,  request=await server.process_request(request))

    @server.is_authenticated('local')
    async def cluster_set_token_key(location, key, **kw):
        return await set_token_key(location, key) ## TODO - Convert to async & await


    @server.api_route('/auth/setup/cluster', methods=['POST'])
    async def cluster_set_service_token_endpoint(service_token: dict, request: Request):
        return await cluster_set_service_token(service_token,  request=await server.process_request(request))

    @server.is_authenticated('local')
    async def cluster_set_service_token(service_token, **kw):
        """
        used primary to update joining nodes with a PYQL_CLUSTER_SERVICE_TOKEN 
        so joining node can pull and set its PYQL_CLUSTER_TOKEN_KEY
        """
        await server.env.set_item(
            'PYQL_CLUSTER_SERVICE_TOKEN', 
            service_token['PYQL_CLUSTER_SERVICE_TOKEN']
        )
        #TODO - Make probe more "async"
        r, rc = await server.probe(
            f"http://{os.environ['PYQL_CLUSTER_SVC']}/auth/key/cluster",
            auth='remote',
            token=await server.env['PYQL_CLUSTER_SERVICE_TOKEN'],
            session=server.session
        )
        if not 'PYQL_CLUSTER_TOKEN_KEY' in r:
            warning = f"error pulling key {r} {rc}"
            return {"error": log.error(warning)}, rc
        set_key, rc = await set_token_key('cluster', r)
        if not rc == 200:
            log.warning(set_key)
        return set_key, rc

    @server.is_authenticated('pyql')
    async def cluster_service_token(tokentype, **kw):
        if tokentype == 'cluster':
            return {"PYQL_CLUSTER_SERVICE_TOKEN": await server.env['PYQL_CLUSTER_SERVICE_TOKEN']}
        if tokentype == 'local':
            return {"PYQL_LOCAL_SERVICE_TOKEN": await server.env['PYQL_LOCAL_SERVICE_TOKEN']}
        server.http_exception(404, "valid token types are cluster or local")

    # Retrieve current local / cluster token keys - requires auth
        
    @server.api_route('/auth/key/{keytype}')
    async def cluster_service_token_key_endpoint(keytype: str, request: Request):
        return await cluster_service_token_key(keytype,  request=await server.process_request(request))
    @server.is_authenticated('pyql')
    async def cluster_service_token_key(keytype, **kw):
        if keytype == 'cluster':
            return {"PYQL_CLUSTER_TOKEN_KEY": await server.env['PYQL_CLUSTER_TOKEN_KEY']}
        if keytype == 'local':
            return {"PYQL_LOCAL_TOKEN_KEY": await server.env['PYQL_LOCAL_TOKEN_KEY']}
        server.http_exception(
            log.error(f"invalid token type specified {tokentype} - use cluster/local"))


    def auth_post_cluster_setup(server):
        """
        run following cluster app initializing so that state_and_quorum_check can be run
        """
        @server.trace
        async def user_register(authtype, user_info, **kw):
            trace = kw['trace']
            pyql = await server.env['PYQL_UUID']
            user_info['type'] = authtype
            required_fields = set()
            # 3 types 'user', 'admin', 'service'
            if authtype == 'user' or authtype == 'admin':
                required_fields = {'username', 'password', 'email'}
            for field in required_fields:
                if not field in user_info:
                    server.http_exception(400, f"missing {field}")
            if 'email' in user_info:
                if not '@' in user_info['email']:
                    server.http_exception(400, f"enter a valid email")
                email_index = user_info['email'].index('@')
                if not '.' in user_info['email'][email_index:]:
                    server.http_exception(400, f"enter a valid email")
                email_check = await server.cluster_table_select(
                    pyql, 'auth', 
                    method='POST', 
                    data={
                        'select': ['id', 'password'], 
                        'where': {'email': user_info['email']}
                    },
                    trace=trace
                )
                if len(email_check['data']) > 0:
                    server.http_exception(
                        400,
                        trace(f"an account with provided email already exists {email_check}"))
                if not len(user_info['password']) >= 8:
                    server.http_exception(400, trace.error(f"password must be a atleast 8 chars"))
                user_info['password'] = encode_password(user_info['password'])
            # User is unique - adding user
            user_info['id'] = str(uuid.uuid1())
            trace(f"creating new user with id {user_info['id']}")
            response, rc = server.cluster_table_insert(pyql, 'auth', user_info, trace=trace)
            if rc == 200:
                if authtype == 'user' or authtype == 'admin':
                    svc, status = await user_register('service', {'parent': user_info['id']})
                    trace(f"creating service account for new user {svc} {status}")
                return {"message": trace(f"user created successfully")}
            return response, rc
        
        @server.api_route('/auth/{authtype}/register', methods=['POST'], status_code=201)
        async def auth_user_register_endpoint(authtype: str, user_info: dict, request: Request):
            return await auth_user_register(authtype,  request=await server.process_request(request))
        @server.state_and_quorum_check
        @server.is_authenticated('pyql')
        @server.trace
        async def auth_user_register(authtype, user_info, **kw):
            return user_register(authtype, user_info, **kw)

        @server.api_route('/auth/token/user')
        async def get_user_auth_token_endpoint(request: Request):
            return await get_user_auth_token( request=await server.process_request(request))
        @server.state_and_quorum_check
        @server.is_authenticated('cluster')
        @server.trace
        async def get_user_auth_token(**kw):
            return {"token": await create_auth_token(kw['authentication'], time.time() + 3600, 'cluster')}


        # Retrieve current local / cluster token - requires auth 
        @server.api_route('/auth/token/join')
        async def cluster_service_join_token_endpoint(request: Request):
            return await cluster_service_join_token( request=await server.process_request(request))
        @server.state_and_quorum_check
        @server.is_authenticated('cluster')
        @server.trace
        async def cluster_service_join_token(**kw):
            trace = kw['trace']
            request = kw['request']
            service_id = await server.cluster_table_select(
                await server.env['PYQL_UUID'],
                'auth', 
                data={
                    'select': '*', 
                    'where': {'parent': kw['authentication'], 'type': 'service'}
                    },
                method='POST',
                quorum=kw['quorum'],
                trace=trace
            )
            trace(f"join token creating for - {service_id}")
            if len(service_id['data']) > 0:
                return {"join": await create_auth_token(service_id['data'][0]['id'], 'join', 'CLUSTER')}
            server.http_exception(400, trace.error(f"unable to find a service account for user"))
        # Retrieve current local / cluster token - requires auth 
        @server.api_route('/auth/token/{tokentype}')
        async def cluster_service_token_endpoint(tokentype: str, request: Request):
            return await cluster_service_token(tokentype,  request=await server.process_request(request))


    server.auth_post_cluster_setup = auth_post_cluster_setup