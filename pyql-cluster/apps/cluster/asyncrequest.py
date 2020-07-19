import asyncio
from aiohttp import ClientSession

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
    async def get_request():
        for request_id, config in request.items():
            print(f"async_get_request with data:  {config}")
            try:
                async with session.get(
                    config['path'],
                    headers=headers_default if not 'headers' in config else config['headers'],
                    timeout=2.0 if not 'timeout' in config else config['timeout']
                ) as r:
                    json_body, status = await r.json(), r.status
            except Exception as e:
                json_body = repr(e)
                status = 408 if 'Timeout' in json_body else 500
            return {request_id: {'content': json_body, 'status': r.status}}
    return await loop.create_task(get_request())
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
    async def post_request():
        for request_id, config in request.items():
            print(f"async_post_request with data:  {config}")
            try:
                async with session.post(
                    config['path'],
                    headers=headers_default if not 'headers' in config else config['headers'],
                    json=config['data'] if 'data' in config else None,
                    timeout=2.0 if not 'timeout' in config else config['timeout']
                ) as r:
                    json_body, status = await r.json(), r.status
            except Exception as e:
                json_body = repr(e)
                status = 408 if 'Timeout' in json_body else 500
            return {request_id: {'content': json_body, 'status': r.status}}
    return await loop.create_task(post_request())

async def async_request_multi(urls, method='GET', loop=None, session=None):
    asyncio.set_event_loop(loop)

    request_results = {}
    request = async_get_request if method == 'GET' else async_post_request
    results = await asyncio.gather(
        *[request(urls[url]['session'], {url: urls[url]}, loop=loop) for url in urls]
        )
    for result in results:
        request_results.update(result)
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
        