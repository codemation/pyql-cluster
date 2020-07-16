
async def db_attach(server):
    db = server.data['cluster']
    await db.create_table(
       'jobs', [
           ('id', str, 'UNIQUE NOT NULL'),
           ('name', str, 'UNIQUE NOT NULL'),
           ('type', str, 'NOT NULL'), #tablesync - cluster
           ('status', str), # QUEUED, RUNNING, WAITING
           ('node', str),
           ('action', str), # What action is called by job
           ('config', str), # What config is used by action
           ('start_time', str),
           ('next_run_time', str),
           ('last_error', str)
       ],
       'id'
    )
    return # Enter db.create_table statement here
            