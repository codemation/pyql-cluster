import asyncio
from aiohttp import ClientSession

global sessions
sessions = {}

async def get_session_by_id(session_id):
    async def session():
        async with ClientSession() as session:
            print(f"client for {session_id} created")
            while True:
                client = yield session
                if client == 'finished':
                    break
    if not session_id in sessions:
        sessions[session_id] = session()
        return await sessions[session_id].asend(None)
    return await sessions[session_id].asend(session_id)
async def cleanup_sessions():
    try:
        for session in sessions:
            await sessions[session].asend('finished')
    except StopAsyncIteration:
        pass

async def get_url(url, session_id):
    session = await get_session_by_id(session_id)
    #session = await session_gen.asend(None)
        
    async with session.get(url) as resp:
        return await resp.json()

async def main():
    url = 'http://192.168.122.100:8090/pyql/node'
    jobs1 = [asyncio.create_task(get_url(url, 'test')) for _ in range(10)]
    jobs2 = [asyncio.create_task(get_url(url, 'test')) for _ in range(10)]

    results = {}
    results['jobs1'] = await asyncio.gather(*jobs1)
    results['jobs2'] = await asyncio.gather(*jobs2)
    await cleanup_sessions()
    print(results)

    return results

loop = asyncio.new_event_loop()
result = loop.run_until_complete(main())

print(result)
print(sessions)

