# pyql-rest
def run(server):
    import os
    from fastapi.testclient import TestClient
    from fastapi.websockets import WebSocket
    ## LOAD ENV Vars & Default values
    environ_vars = [
            {'PYQL_DEBUG': True} # TODO - Make this disable by default
        ]
    for env in environ_vars:
        for e, v in env.items(): 
            if e in os.environ:
                t = type(v)
                try:
                    setattr(server, e, t(os.environ[e]))
                except Exception as e:
                    print(f"unable to set {e} from env vars, not a {t} capable value {os.environ[e]}, using default value {v}")
                    setattr(server, e, v)
            else:
                setattr(server, e, v)
    try:
        server.data = dict()
        server.jobs = []
        cmd_dir_path = None
        real_path = None
        with open('./.cmddir', 'r') as cmddir:
            for line in cmddir:
                cmd_dir_path = line
            real_path = str(os.path.realpath(cmddir.name)).split('.cmddir')[0]
        if not real_path == cmd_dir_path:
            print(f"NOTE: Project directory may have moved, updating project cmddir files from {cmd_dir_path} -> {real_path}")
            import os
            os.system("find . -name .cmddir > .proj_cmddirs")
            with open('.proj_cmddirs', 'r') as proj_cmd_dirs:
                for f in proj_cmd_dirs:
                    with open(f.rstrip(), 'w') as proj_cmd:
                        proj_cmd.write(real_path)
    except Exception as e:
        print("encountered exception when checking proj_path")
        print(repr(e))
    

    from logs import setup as log_setup
    log_setup.run(server)

    log = server.log
    
    async def cluster_setup():

        from dbs import setup as db_setup # TOO DOO -Change func name later
        await db_setup.run(server) # TOO DOO - Change func name later
        from apps import setup
        await setup.run(server)
        from events import setup as event_setup
        await event_setup.run(server)
        return {"message": log.info("cluster setup completed")}

    @server.websocket_route("/cluster_setup")
    async def attach_databases(websocket: WebSocket):
        await websocket.accept()
        response = await cluster_setup()
        await websocket.send_json({"message": log.info(response)})
        await websocket.close()
    def trigger_cluster_setup():
        client = TestClient(server)
        with client.websocket_connect("/cluster_setup") as websocket:
            return websocket.receive_json()
    trigger_cluster_setup()