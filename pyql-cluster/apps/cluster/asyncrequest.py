import asyncio
import requests
from aiohttp import ClientSession

async def async_get_request(session, url):
    for urlId, config in url.items():
        #print(f"{url} started")
        try:
            async with session.get(
                config['path'] if 'http' in config['path'] else f"http://{config['path']}",
                headers={'Accept': 'application/json', 'Content-Type': 'application/json'}
                ) as r:
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
                headers={'Accept': 'application/json', "Content-Type": "application/json"},
                json=config['data'] if 'data' in config else None
            ) as r:
                jsonBody = await r.json()
            return urlId, {'content': jsonBody, 'status': r.status}
        except Exception as e:
            return urlId, {'content': str(repr(e)), 'status': 400}

async def async_request_pool(urls, method):
    async with ClientSession() as session:
        if method == 'GET':
            return await asyncio.gather(*[async_get_request(session, {url: urls[url]}) for url in urls])
        elif method == 'POST':
            return await asyncio.gather(*[async_post_request(session, {url: urls[url]}) for url in urls])


def async_request(urls, method='GET'):
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
    result = asyncio.run(async_request_pool(urls,method))
    return {r[0]: r[1] for r in result}