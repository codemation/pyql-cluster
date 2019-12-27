
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'jobs', [
           ('id', str, 'UNIQUE'), 
           ('type', str), #tablesync - cluster
           ('status', str), # QUEUED, RUNNING, WAITING
           ('node', str),
           ('config', str),
           ('start_time', str),
           ('lastError', str)
       ],
       'id'
    )
    pass # Enter db.create_table statement here
            