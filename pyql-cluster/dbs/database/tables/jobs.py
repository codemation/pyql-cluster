
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'jobs', [
           ('id', str, 'UNIQUE NOT NULL'),
           ('name', str, 'NOT NULL'),
           ('type', str, 'NOT NULL'), #tablesync - cluster
           ('status', str), # QUEUED, RUNNING, WAITING
           ('node', str),
           ('config', str),
           ('start_time', str),
           ('next_run_time', str),
           ('lastError', str)
       ],
       'id'
    )
    pass # Enter db.create_table statement here
            