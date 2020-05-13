import asyncio
import requests
from aiohttp import ClientSession

headersDefault = {'Accept': 'application/json', "Content-Type": "application/json"}

async def async_get_request(session, url):
    for urlId, config in url.items():
        #print(f"{url} started")
        try:
            """
            async with session.get(
                config['path'] if 'http' in config['path'] else f"http://{config['path']}",
                headers=headersDefault if not 'headers' in config else config['headers'],
                timeout=2.0 if not 'timeout' in config else config['timeout']
                ) as r:
            """
            r = await session.get(
                config['path'] if 'http' in config['path'] else f"http://{config['path']}",
                headers=headersDefault if not 'headers' in config else config['headers'],
                timeout=2.0 if not 'timeout' in config else config['timeout']
            )
            jsonBody = await r.json()
            #print(f"{url} completed")
            return urlId, {'content': jsonBody, 'status': r.status}
        except Exception as e:
            return urlId, {'content': str(repr(e)), 'status': 400}
async def async_post_request(session, url):
    for urlId, config in url.items():
        #print(f"{url} started")
        try:
            async with session.post(
                config['path'] if 'http' in config['path'] else f"http://{config['path']}",
                headers=headersDefault if not 'headers' in config else config['headers'],
                json=config['data'] if 'data' in config else None,
                timeout=2.0 if not 'timeout' in config else config['timeout']
            ) as r:
                jsonBody = await r.json()
            return urlId, {'content': jsonBody, 'status': r.status}
        except Exception as e:
            return urlId, {'content': str(repr(e)), 'status': 400}

async def async_request_pool(urls, method, session=None, result=None):
    #if session ==None:
    #    session = ClientSession()
    async with ClientSession() as session:
        try:
            if method == 'GET':
                results = []
                for url in urls:
                    results.append(asyncio.create_task(async_get_request(session, {url: urls[url]},)))
                
                result.result =  await asyncio.gather(*results)
                """
                getTask = asyncio.create_task([async_get_request(session, {url: urls[url]}, ) for url in urls][0])
                getTask.result()


                result.result = await asyncio.gather(*[async_get_request(session, {url: urls[url]}, ) for url in urls])
                """
            elif method == 'POST':
                result.result = await asyncio.gather(*[async_post_request(session, {url: urls[url]}) for url in urls])
        except Exception as e:
            await asyncio.sleep(1)

class async_result:
    pass


def async_request(urls, method='GET', loop=None, session=None):
    """
        usage: 
        urls = [
    {
        'url1': {
            'path': 'http://netapp.com'
            'data':
        }
    },
    """
    if loop == None:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    r = async_result()
    asyncio.run(async_request_pool(urls,method, session, result=r))
    #result = asyncio.run()
    return {r[0]: r[1] for r in r.result}