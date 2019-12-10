# internal
def run(server):
    import os

    def db_check(database):
        db = server.data[database]
        if db.type == 'sqlite':
            result = db.get(f"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = [t[0] for t in result]
            if database == 'cluster':
                clusterTables = ['clusters', 'endpoints', 'databases', 'tables', 'state', 'pyql']
                for index, check in enumerate(clusterTables):
                    print(f"checking {check}")
                    if not check in tables:
                        print(f"missing {check}")
                        return {'message': f"missing table {check} in database {database}"}, 500
                    
                    
            for r in result:
                print(r)
            pass
            #tables = db.tables:
        else:    
            tables = db.run('show tables')
            print(f"db_check result: {tables}")
            for table in tables:
                if not table[0] in server.data[database].tables:
                    server.data[database].load_tables()
        return {"messages": f"{database} status ok", "tables": list(server.data[database].tables.keys())}, 200
    

    @server.route('/internal/job')
    def internal_job_queue():
        print(server.jobs)
        return server.jobs.pop(0) if len(server.jobs) > 0 else {"status": 200, "message": "no jobs in queue"}, 200 
    @server.route('/internal/jobs')
    def internal_list_job_queue():
        return {'jobs': server.jobs}, 200

    @server.route('/internal/db/check')
    def internal_db_check():
        messages = []
        for database in server.data:
            messages.append(db_check(database))
        return {"result": messages if len(messages) > 0 else "No databases attached", "jobs": server.jobs}, 200
    @server.route('/internal/db/<database>/status')
    def internal_db_status(database):
        if database in server.data:
            return db_check(database)
        else:
            return {"status": 404, "message": f"database with name {database} not found"}, 404
    
    #@server.route('/internal/db/attach')
    #This route is handled in dbs/database/database_db.py

    @server.route('/internal/cluster/status')
    def internal_cluster_status():
        db = server.data[os.environ['DB_NAME']]
        print("Hello internal World") 
        return "<h1>Hello internal World</h1>", 200
    @server.route('/internal/cluster/join')
    def internal_cluster_join():
        db = server.data[os.environ['DB_NAME']]
        print("Hello internal World") 
        return "<h1>Hello internal World</h1>", 200
