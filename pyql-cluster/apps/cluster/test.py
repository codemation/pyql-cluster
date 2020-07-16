import asyncio
#from aiohttp import ClientSession

async def work(level, seconds):
    print(f"{level}_{seconds} started")
    await asyncio.sleep(seconds)
    print(f"{level}_{seconds} completed")
    return {"work": f"{level}_{seconds} completed"}


async def level3():
    results = {}
    print(f"starting level3")
    jobs = []
    for job in [2, 5, 10]:
        jobs.append(asyncio.create_task(work('level3', job)))
    results['level3'] = await asyncio.gather(*jobs)
    results['level2'] = await level2()
    results['level1'] = await level1()
    return results


async def level2():
    results = {}
    print(f"starting level2")
    jobs = []
    for job in [2, 5]:
        jobs.append(asyncio.create_task(work('level2', job)))
    results['level2'] = await asyncio.gather(*jobs)
    results['level1'] = await level1()
    return results

async def level1():
    results = {}
    print(f"starting level1")
    jobs = []
    for job in [2]:
        jobs.append(asyncio.create_task(work('level1', job)))
    results['level1'] = await asyncio.gather(*jobs)
    return results

async def main():
    jobs = [asyncio.create_task(task()) for task in [level1, level2, level3]]
    print(f"jobs: {jobs}")
    return await asyncio.gather(*jobs)

loop = asyncio.new_event_loop()
print(loop.jobs)
loop.run_until_complete(main())

