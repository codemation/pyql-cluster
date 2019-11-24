# internal
def run(server):
    import os
    @server.route('/internal/db/check')
    def internal_db_check():
        db = server.data[os.environ['DB_NAME']]
        tables = db.run('show tables')
        print(tables)
        for table in tables:
            if not table[0] in server.data[os.environ['DB_NAME']].tables:
                server.data[os.environ['DB_NAME']].load_tables()
        return {"message": f"{os.environ['DB_NAME']} status ok"}, 200
    @server.route('/internal/db/status')
    def internal_db_status():
        db = server.data[os.environ['DB_NAME']]
        print("Hello internal World") 
        return "<h1>Hello internal World</h1>", 200
    
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
