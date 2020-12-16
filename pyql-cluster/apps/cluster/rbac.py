async def run(server):

    log = server.log
    
    async def get_clusterid_by_name_authorized(cluster_name, **kwargs):
        log.warning(f"get_clusterid_by_name_authorized {kwargs}")
        if 'cluster_allowed' in kwargs:
            return kwargs['cluster_allowed']
        request = kwargs['request']
        user_id = request.auth
        log.warning(f"check_user_access called for cluster {cluster_name} using {user_id}")
        clusters = await server.data['cluster'].tables['clusters'].select('*', where={'name': cluster_name})
        log.warning(f"get_clusterid_by_name_authorized clusters: {clusters}")
        cluster_allowed = None
        for cluster in clusters:
            log.warning(f"checking {user_id} {cluster['owner']}")
            if user_id == cluster['owner'] or user_id in cluster['access']['allow']:
                cluster_allowed = cluster['id']
                break
            if 'auth_children' in kwargs:
                for child_id in kwargs['auth_children']:
                    if child_id == cluster['owner'] or child_id in cluster['access']['allow']:
                        cluster_allowed = cluster['id']
                        break
        if cluster_allowed == None:
            env = kwargs
            warning = f"user {user_id} access to cluster with name {cluster_name}, no cluster was found which user has access rights or none exists - env {env}"
            server.http_exception(404, log.warning(warning))
        return str(cluster_allowed)

    server.get_clusterid_by_name_authorized = get_clusterid_by_name_authorized

    def cluster_name_to_uuid(func):
        """
        After authenticaion, uses 'userid' in server.auth to check
        if user has "access" to a cluster with name in kw['cluster']
        and replaces string name with 'uuid' of cluster if exists
        """
        async def check_user_access(*args, **kwargs):
            args = list(args)
            request = kwargs['request']
            log.warning(f"check_user_access - args {args} - {kwargs}")

            if 'cluster_allowed' in kwargs:
                if f"{kwargs['cluster_allowed']}_log" == args[0]:
                    args[0] = kwargs['cluster_allowed']
                if 'cluster' in kwargs and f"{kwargs['cluster_allowed']}_log" == kwargs['cluster']:
                    kwargs['cluster'] = kwargs['cluster_allowed']
                if 'table' in kwargs and kwargs['table'] == kwargs['table_allowed'] or args[1] == kwargs['table_allowed']:
                    return await func(*args, **kwargs)

            if not 'auth' in request.__dict__:
                log.error("authentication is required or missing, this should have been handled by is_authenticated")
                server.http_exception(500, "authentication is required or missing")
            
            cluster_name = kwargs['cluster'] if 'cluster' in kwargs else args[0]

            kwargs['cluster'] = await get_clusterid_by_name_authorized(
                cluster_name, **kwargs)
            args[0] = kwargs.pop('cluster') if cluster_name == args[0] else args[0]
            kwargs['cluster_name'] = cluster_name
            request.cluster_name = cluster_name
            args = tuple(args)
            return await func(*args, **kwargs)
        return check_user_access
    server.cluster_name_to_uuid = cluster_name_to_uuid

    @server.trace
    async def get_auth_http_headers(location=None, token=None, **kw):
        trace=kw['trace']
        if token == None:
            auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not location == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
            trace.warning(f"get_auth_http_headers called using location: {location} - token: {token} - {kw} auth: {auth}")
            token = kw['token'] if 'token' in kw else None
            token = await server.env[auth] if token == None else token
        headers = {
            'Accept': 'application/json', "Content-Type": "application/json",
            "authorization": f"Bearer {token}"}
        trace.warning(f"get_auth_http_headers {headers}")
        return headers
    server.get_auth_http_headers = get_auth_http_headers