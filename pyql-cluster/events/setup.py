async def run(server):
    import asyncio, uuid, uvloop
    from fastapi.websockets import WebSocket
    from fastapi.testclient import TestClient

    log = server.log
    server.tasks = []
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

    async def worker(client, interval):
        """
        waits for new work in queue & sleeps
        """
        loop = asyncio.get_running_loop()
        log.warning(f"worker {client} started within loop {loop} - {loop is server.event_loop}")
        try:
            while True:
                if len(server.tasks) == 0:
                    log.warning(f"worker {client} found no tasks to run, sleeping {interval}")
                    await asyncio.sleep(interval)
                    continue

                # Pulls job off top of stack to reserve 
                job = server.tasks.pop(0)
                if job == 'finished':
                    return
                try:
                    result = await job()
                except Exception as e:
                    log.exception(f"worker encountered exception when running {job}")
                server.tasks.append(job)
                log.warning(f"worker finished job: {result}")
                await asyncio.sleep(interval)
        except Exception as e:
            log.exception(f"unandled exception or application is shutting down, worker {client} exiting")
    

    # Worker Websocket endpoint - This enables websockets to drive worker 
    # tasks in the background, not dependent on a subprocess / request thread

    @server.websocket_route("/worker")
    async def run_worker(websocket: WebSocket):
        await websocket.accept()
        config = await websocket.receive_json()
        client, interval = config['client'], config['interval']
        await websocket.send_json({"message": f"started worker {client} with config {config}"})
        try: 
            await worker(client, interval)
        except Exception as e:
            print(repr(e))
        finally:
            log.warning(f"worker {client} exiting")
            await cleanup_client(config['client'])
            await websocket.close()

    async def add_worker(interval: int):
        import time
        start = time.time()
        client_id = str(uuid.uuid1())
        # pull open websocket client conection
        websocket = await get_client_by_id(client_id, "/worker")
        try:
            # send work to websocket server
            websocket.send_json({"interval": interval, 'client': client_id})
            
            # waits for ack of work received
            result =  websocket.receive_json()
            log.warning(f"started worker: {result} after {time.time()-start} seconds")
        except Exception:
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
        
        for _ in range(2):
            await add_worker(10)

    @server.on_event("shutdown")
    async def close_sessions():
        """
        on app shutdown, closes workers & exits open websocket context
        """
        server.tasks = []
        for client in server.clients:
            server.tasks.append('finished')
            await cleanup_client(client)


    print("running events")

    from events.health import health
    await health.run(server)

    from events.jobs import jobs
    await jobs.run(server)

    from events.cluster import cluster
    await cluster.run(server)

    await start_workers()