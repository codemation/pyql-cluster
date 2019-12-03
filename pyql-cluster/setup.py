# pyql-rest
def run(server):
    try:
        server.data = dict()
        server.jobs = []
        import os
        cmddirPath = None
        realPath = None
        with open('./.cmddir', 'r') as cmddir:
            for line in cmddir:
                cmddirPath = line
            realPath = str(os.path.realpath(cmddir.name)).split('.cmddir')[0]
        if not realPath == cmddirPath:
            print(f"NOTE: Project directory may have moved, updating project cmddir files from {cmddirPath} -> {realPath}")
            import os
            os.system("find . -name .cmddir > .proj_cmddirs")
            with open('.proj_cmddirs', 'r') as projCmdDirs:
                for f in projCmdDirs:
                    with open(f.rstrip(), 'w') as projCmd:
                        projCmd.write(realPath)
    except Exception as e:
        print("encountered exception when checking projPath")
        print(repr(e))
    

    from dbs import setup as db_setup # TOO DOO -Change func name later
    db_setup.run(server) # TOO DOO - Change func name later
    from apps import setup
    setup.run(server)
    from events import setup as event_setup
    event_setup.run(server)
    
    try:
        #from apps import setup
        #setup.run(server)
        #from dbs import setup as db_setup # TOO DOO -Change func name later
        #db_setup.run(server) # TOO DOO - Change func name later
        #from events import setup as event_setup
        #event_setup.run(server)
        pass

    except Exception as e:
        print("Project may not have any apps configured or apps setup.py cannot be found")
        print(repr(e))
            