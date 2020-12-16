async def run(server):
    import asyncio, uuid
    from fastapi.websockets import WebSocket
    from fastapi.testclient import TestClient
    from collections import deque

    log = server.log
    
    # generic tasks which are not time-sensitive
    server.tasks = asyncio.Queue()

    # txn_signals are added when a cluster_table_change is called
    # to signal table_endpoints that there are new txns to /flush
    server.txn_signals = asyncio.Queue()

    # tasks are added when /db/<database>/table/<table>/flush is called 
    # which attempts to sync the table with the latest txns in 
    server.flush = asyncio.Queue()

    server.cron_jobs = asyncio.Queue()

    server.server_tasks = []

    def cron(interval: int):
        def cron_setup(f):
            async def cron_job():
                await f()
                await asyncio.sleep(interval)
                await server.cron_jobs.put(cron_job)
            asyncio.create_task(server.cron_jobs.put(cron_job))
            return f
        return cron_setup
    
    server.cron = cron

    async def tasks_worker():
        log.warning(f"starting tasks_worker")
        while True:
            try:
                task = await server.tasks.get()
                result = await task()
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    log.exception(f"error with task worker")
                    continue
                break

    async def txn_signals_worker():
        log.warning(f"starting txn_signals_worker")
        while True:
            try:
                job = await server.txn_signals.get()
                result = await job[0]
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    log.exception(f"error with txn_signals worker")
                    continue
                break

    async def cron_worker():
        log.warning(f"starting cron_worker")
        while True:
            try:
                job = await server.cron_jobs.get()
                result = await job()
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    log.exception(f"error with cron worker")
                    continue
                break

    async def flush_worker():
        log.warning(f"starting flush_worker")
        while True:
            try:
                flush_job = await server.flush.get()
                flush = await flush_job()
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    log.exception(f"error with flush_job")
                    continue
                break

    # start workers and store in server.server_tasks
    for worker in {tasks_worker, txn_signals_worker, cron_worker, flush_worker}:
        server.server_tasks.append(
            asyncio.create_task(worker())
    )
    
    @server.on_event("shutdown")
    async def close_sessions():
        """
        on app shutdown, closes workers & exits open websocket context
        """
        for task in server.server_tasks:
            task.cancel()


    print("running events")

    from events.health import health
    await health.run(server)

    from events.jobs import jobs
    await jobs.run(server)

    from events.cluster import cluster
    await cluster.run(server)