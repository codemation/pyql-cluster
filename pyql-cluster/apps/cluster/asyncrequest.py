import asyncio
from aiohttp import ClientSession

headers_default = {'Accept': 'application/json', "Content-Type": "application/json"}

async def async_get_request(session: ClientSession, request: dict):
    """
    Usage:
        request - {
            'request_id': {
                'path': 'http://google.com',
                'headers': {'Authentication': 'Token abcd12344123'}
            }
        }

    """
    for request_id, config in request.items():
        try:
            r = await session.get(
                config['path'],
                headers=headers_default if not 'headers' in config else config['headers'],
                timeout=2.0 if not 'timeout' in config else config['timeout']
            )
            json_body = await r.json()
            return {request_id: {'content': json_body, 'status': r.status}}
        except Exception as e:
            return {request_id: {'content': str(repr(e)), 'status': 400}}
async def async_post_request(session: ClientSession, request: dict):
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
    for request_id, config in request.items():
        try:
            print(f"async_post_request with data:  {config}")
           r = await session.post(
                config['path'],
                headers=headers_default if not 'headers' in config else config['headers'],
                json=config['data'] if 'data' in config else None,
                timeout=2.0 if not 'timeout' in config else config['timeout']
            )
            json_body = await r.json()
            return {request_id: {'content': json_body, 'status': r.status}}
        except Exception as e:
            return {request_id: {'content': str(repr(e)), 'status': 400}}

async def async_request_multi(urls, method='GET', loop=None, session=None):
    if session ==None:
        session = ClientSession(loop=loop)
    async with session:
        requests = {}
        if method == 'GET':
            for request in await asyncio.gather(*[async_get_request(session, {url: urls[url]}) for url in urls]):
                requests.update(request)
            return requests
        for request in await asyncio.gather(*[async_post_request(session, {url: urls[url]}) for url in urls]):
            requests.update(request)
        return requests

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
    r = async_result()
    result = loop.run_until_complete(async_request_multi(urls, method, loop=loop))
    return {r[0]: r[1]['status'] for r in result}

def test_async_request():
    urls = {
            'netapp': {
                'path': 'http://netapp.com'
            },
            'google': {
                'path': 'http://google.com'
            },
            'netflix': {
                'path': 'http://netflix.com'
            }
        }
    result = async_request(urls)
    print(result)

if __name__ == '__main__':
    test_async_request()
        