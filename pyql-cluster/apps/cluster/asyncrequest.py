import asyncio
from aiohttp import ClientSession, ClientTimeout

headers_default = {'Accept': 'application/json', "Content-Type": "application/json"}

async def async_get_request(session: ClientSession, request: dict, loop=None):
    """
    Usage:
        request - {
            'request_id': {
                'path': 'http://google.com',
                'headers': {'Authentication': 'Token abcd12344123'}
            }
        }

    """
    asyncio.set_event_loop(loop)
    #loop = asyncio.get_running_loop()
    async def get_request():
        for request_id, config in request.items():
            print(f"async_get_request with data:  {config}")
            try:
                timeout=None
                if 'timeout' in config:
                    timeout = ClientTimeout(connect=config['timeout'])
                async with session.get(
                    config['path'],
                    headers=headers_default if not 'headers' in config else config['headers'],
                    timeout=timeout
                ) as r:
                    json_body, status = await r.json(), r.status
            except Exception as e:
                json_body = repr(e)
                status = 408 if 'Timeout' in json_body else 500
            return {request_id: {'content': json_body, 'status': status}}
    return await loop.create_task(get_request())
    #return await asyncio.create_task(get_request())
async def async_post_request(session: ClientSession, request: dict, loop=None):
    """
    Usage:
        request - {
            'request_id': {
                'path': 'http://google.com',
                'headers': {'Authentication': 'Token abcd12344123'},
                'data': {"foo":"bar"}
            }
        }
    """
    asyncio.set_event_loop(loop)
    #loop = asyncio.get_running_loop()
    async def post_request():
        for request_id, config in request.items():
            print(f"async_post_request with data:  {config}")
            try:
                timeout=None
                if 'timeout' in config:
                    timeout = ClientTimeout(connect=config['timeout'])
                async with session.post(
                    config['path'],
                    headers=headers_default if not 'headers' in config else config['headers'],
                    json=config['data'] if 'data' in config else None,
                    timeout=timeout
                ) as r:
                    json_body, status = await r.json(), r.status
            except Exception as e:
                json_body = repr(e)
                status = 408 if 'Timeout' in json_body else 500
            return {request_id: {'content': json_body, 'status': status}}
    return await loop.create_task(post_request())
    #return await asyncio.create_task(post_request())

async def async_request_multi(urls, method='GET', loop=None, session=None):
    if session == 'new':
        session = ClientSession()
        for url in urls:
            urls[url]['session'] = session

    asyncio.set_event_loop(loop)
    request_results = {}
    request = async_get_request if method == 'GET' else async_post_request
    results = await asyncio.gather(
        *[request(urls[url]['session'], {url: urls[url]}, loop=loop) for url in urls],
        loop=loop,
        return_exceptions=True
        )
    for result in results:
        if isinstance(result, dict):
            request_results.update(result)
        else:
            if not 'errors' in request_results:
                request_results['errors'] = []
            request_results['errors'].append(result)
    return request_results

def async_request(urls, method='GET', loop=None, session=None):
    """
    ## Used if called by synchronus function
        usage: 
        urls = {
            'url1': {
                'path': 'http://netapp.com'
                'data':
            },
            'url2': {
                ..
            }
    },
    """
    if loop == None:
        loop = asyncio.new_event_loop()
    result = loop.run_until_complete(async_request_multi(urls, method, loop=loop, session='new'))
    return result

def test_async_request():
    urls = {
            'netapp': {
                'path': 'http://192.168.3.33:8090/pyql/node'
            },
            'google': {
                'path': 'http://192.168.3.33:8090/pyql/node'
            },
            'netflix': {
                'path': 'http://192.168.3.33:8090/pyql/node'
            }
        }
    result = async_request(urls)
    print(result)

if __name__ == '__main__':
    test_async_request()
        