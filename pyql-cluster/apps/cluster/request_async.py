import asyncio, concurrent, os
import requests, json

async def main(urls, action, loop=None):
    if loop == None:
        loop = asyncio.get_running_loop()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, os.cpu_count() + 4)) as executor:
        futures = [
            loop.run_in_executor(executor, action, url, urls[url]['path'], urls[url]) for url in urls
        ]
        results = await asyncio.gather(*futures)
    response = {}
    for d in results:
        for k,v in d.items():
            response[k] = v
    return response
def get_requester(method):
    if method == 'POST':
        def requests_post_results(urlId, url, config):
            action = requests
            if 'path' in config:
                config.pop('path')
            if 'data' in config:
                config['data'] = json.dumps(config['data'])
            if 'session' in config:
                action = config['session']
                config.pop('session')
            try:
                print(f"started requests_post_results for {urlId} with config: {config}")
                r = action.post(url, **config)
                print(f"finished requests_post_results for {urlId}")
                return {urlId: {'content':r.json(), 'status': r.status_code}}
            except Exception as e:
                error = repr(e)
                try:
                    print(f"finished requests_post_results for {urlId} with exception: {repr(e)} on {r.text}")
                    return {urlId: {'content': r.text, 'status': r.status_code, "error": repr(e)}}
                except Exception:
                    return {urlId:  {'content': error, 'status': 500, 'error': error}}
        return requests_post_results
    if method == 'GET':
        def requests_get_results(urlId, url, config):
            action = requests
            if 'path' in config:
                config.pop('path')
            if 'session' in config:
                action = config['session']
                config.pop('session')
            try:
                print(f"started requests_get_results for {urlId} with config: {config}")
                r = action.get(url, **config)
                print(f"finished requests_get_results for {urlId}")
                return {urlId:  {'content': r.json(), 'status': r.status_code}}
            except Exception as e:
                error = repr(e)
                try:
                    print(f"finished requests_get_results for {urlId} with exception: {repr(e)} on {r.text}")
                    return {urlId:  {'content': r.text, 'status': r.status_code, "error": repr(e)}}
                except Exception:
                    return {urlId:  {'content': error, 'status': 500, 'error': error}}
        return requests_get_results
            
def requests_async(urls, method='GET', loop=None):
    """
    urls= {'requestId': {'path: 'http://google.com', 'data': {}, 'headers': headers}}
    """
    if loop == None:
        loop = asyncio.new_event_loop()

    action = get_requester(method)
    return loop.run_until_complete(main(urls,action, loop))
        

    
    