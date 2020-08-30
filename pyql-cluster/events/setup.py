async def run(server):
    import asyncio, uuid, uvloop
    from fastapi.websockets import WebSocket
    from fastapi.testclient import TestClient
    from collections import deque

    log = server.log
    
    # generic tasks which are not time-sensitive
    server.tasks = deque()

    # txn_signals are added when a cluster_table_change is called
    # to signal table_endpoints that there are new txns to /flush
    server.txn_signals = deque()

    # tasks are added when /db/<database>/table/<table>/flush is called 
    # which attempts to sync the table with the latest txns in 
    server.flush = deque()

    server.clients = {}

    # websocket client factory

    async def get_client_by_id(client_id: str, path: str):
        """
        creates an async generator for holding the context 
        of a TestClient.websocket_connect open 
        """
        async def client():
            log.warning(f"started client with id: {client_id} and path {path}")
            c = TestClient(server)
            with c.websocket_connect(f"{path}") as websocket:
                while True:
                    result = yield websocket
                    if result == 'finished':
                        log.warning(f"cleaning up client with id: {client_id} and path {path}")
                        break
        # returns open connetion, if exists
        if not client_id in server.clients:
            # creates & assigns generator to server.clients
            server.clients[client_id] = client()
            # initializes generator with .asend(None) & returns open websocket
            return await server.clients[client_id].asend(None)
        return await server.clients[client_id].asend(client_id)

    # Workers monitoring for tasks in server.taskss

    async def worker(client, interval, queue):
        """
        waits for new work in queue & sleeps
        queue:
            'tasks'
            'flush_tasks'
            'txn_signals'
            'flush'
        """
        loop = asyncio.get_running_loop()
        log.warning(f"worker {client} started within loop {loop} - {loop is server.event_loop}")
        try:
            while True:
                if len(server.__dict__[queue]) == 0:
                    #log.warning(f"worker {client} found no tasks to run, sleeping {interval}")
                    await asyncio.sleep(interval)
                    continue
                # Pulls job off top of stack to reserve 
                job = server.__dict__[queue].popleft()
                cron = True
                restart = False
                if job == 'finished':
                    return
                try:
                    result = await job()
                except Exception as e:
                    result = log.exception(f"{queue} worker encountered exception when running {job}")
                    restart = True
                if queue == 'tasks' or restart:
                    if restart and queue == 'flush':
                        server.__dict__[queue].leftappend(job)
                    else:
                        server.__dict__[queue].append(job)
                log.warning(f"{queue} worker finished job: {result} - queue: {server.__dict__[queue]}")
                await asyncio.sleep(interval)
        except Exception as e:
            log.exception(f"unandled exception or application is shutting down, worker {client} exiting")
    

    # Worker Websocket endpoint - This enables websockets to drive worker 
    # tasks in the background, not dependent on a subprocess / request thread

    @server.websocket_route("/worker")
    async def run_worker(websocket: WebSocket):
        await websocket.accept()
        config = await websocket.receive_json()
        client, interval, queue = config['client'], config['interval'], config['queue']
        await websocket.send_json({"message": f"started worker {client} with config {config}"})
        try: 
            await worker(client, interval, queue)
        except Exception as e:
            print(repr(e))
        finally:
            log.warning(f"worker {client} exiting")
            await cleanup_client(config['client'])
            await websocket.close()

    async def add_worker(interval: int, queue: str):
        import time
        start = time.time()
        client_id = str(uuid.uuid1())
        # pull open websocket client conection
        websocket = await get_client_by_id(client_id, "/worker")
        try:
            # send work to websocket server
            websocket.send_json({"interval": interval, 'client': client_id, 'queue': queue})
            
            # waits for ack of work received
            result =  websocket.receive_json()
            log.warning(f"started worker: {result} after {time.time()-start} seconds")
        except Exception as e:
            log.exception("Exceptoin during add_worker")
            await cleanup_client(client_id)
        return result

    server.add_worker = add_worker

    async def cleanup_client(client_id):
        """
        cleanup open websocket connection
        """
        if client_id in server.clients:
            try:
                await sever.clients[client_id].asend('finished')
            except Exception:
                print(f"I cleaned up client with {client_id}")
    server.cleanup_client = cleanup_client
    
    #@server.on_event("startup")
    async def start_workers():
        """
        creates default running workers on app start
        """
        await add_worker(0.001, 'flush')
        for _ in range(4):
            await add_worker(0.001, 'txn_signals')
            await add_worker(5, 'tasks')
            
            

    @server.on_event("shutdown")
    async def close_sessions():
        """
        on app shutdown, closes workers & exits open websocket context
        """
        for queue in [
            'tasks', 
            'txn_signals',
            'flush'
            ]:
            server.__dict__[queue] = deque()
            for client in server.clients:
                server.__dict__[queue].append('finished')
        for client in server.clients:
            await cleanup_client(client)

    print("running events")

    from events.health import health
    await health.run(server)

    from events.jobs import jobs
    await jobs.run(server)

    from events.cluster import cluster
    await cluster.run(server)

    await start_workers()