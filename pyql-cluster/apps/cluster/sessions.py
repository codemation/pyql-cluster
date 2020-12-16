async def run(server):
    import asyncio
    from aiohttp import ClientSession
    from apps.cluster.asyncrequest import async_request_multi, async_get_request, async_post_request

    log = server.log

    server.sessions = {}

    @server.trace
    async def get_endpoint_sessions(endpoint, **kw):
        """
        pulls endpoint session if exists else creates & returns
        """
        trace = kw['trace']
        loop = server.event_loop if not 'loop' in kw else kw['loop']
        async def session():
            async with ClientSession(loop=loop) as client:
                trace(f"started session for endpoint {endpoint}")
                while True:
                    status = yield client
                    if status == 'finished':
                        trace(f"finished session for endpoint {endpoint}")
                        break
        if not endpoint in server.sessions:
            server.sessions[endpoint] = [{'session': session(), 'loop': loop}]
            return await server.sessions[endpoint][0]['session'].asend(None)
        for client in server.sessions[endpoint]:
            if loop == client['loop']:
                return await client['session'].asend(endpoint)
        log.warning("session existed but not for this event loop, creating")
        client = session()
        server.sessions[endpoint].append({'session': client, 'loop': loop})
        return await client.asend(None)

    # attaching to global server var    
    server.get_endpoint_sessions = get_endpoint_sessions

    # creating initial session for this nodeq
    await server.get_endpoint_sessions(server.PYQL_NODE_ID)

    @server.trace
    async def probe(path, method='GET', data=None, timeout=5.0, auth=None, headers=None, **kw):
        trace = kw['trace']
        auth = 'PYQL_CLUSTER_SERVICE_TOKEN' if not auth == 'local' else 'PYQL_LOCAL_SERVICE_TOKEN'
        headers = await server.get_auth_http_headers(auth, **kw) if headers == None else headers
        session, temp_session, temp_id = None, False, None
        loop = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']

        if not 'session' in kw:
            temp_session, temp_id = True, str(uuid.uuid1())
            session = await server.get_endpoint_sessions(temp_id, loop=loop)
        else:
            session = kw['session']
            
        url = f'{path}'
        try:
            request = {
                'probe': {
                    'path': url,
                    'headers': headers,
                    'timeout': timeout
                }
            }
            if method == 'GET':
                result = await async_get_request(session, request, loop=loop) 
            else:
                request['probe']['data'] = data
                result = await async_post_request(session, request, loop=loop)
            result, status = result['probe']['content'], result['probe']['status']
        except Exception as e:
            error = f"Encountered exception when probing {path} - {repr(e)}"
            result, status = {"error": trace.error(error)}, 500
        trace(f"request: {request} - result: {result}")
        if temp_session:
            await cleanup_session(temp_id)
        return result, status
    server.probe = probe

    async def cleanup_session(endpoint):
        if endpoint in server.sessions:
            log.warning(f"removing session for endpoint {endpoint}")
            try:
                for session in server.sessions[endpoint]:
                    try:
                        await session['session'].asend('finished')
                    except StopAsyncIteration:
                        pass
                del server.sessions[endpoint]
            except Exception as e:
                log.exception("exception when cleaning up session")
    async def cleanup_sessions():
        for endpoint in server.sessions:
            await cleanup_session(endpoint)
        return
    server.cleanup_sessions = cleanup_sessions