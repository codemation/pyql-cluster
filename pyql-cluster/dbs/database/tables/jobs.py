
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'jobs', [
           ('id', str, 'UNIQUE'),
           ('name', str, 'UNIQUE'),
           ('type', str), #tablesync - cluster
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
            