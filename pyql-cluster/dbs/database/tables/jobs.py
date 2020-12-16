
async def db_attach(server):
    db = server.data['cluster']
    if 'jobs' in db.tables:
        await db.run('drop table jobs')
    await db.create_table(
       'jobs', [
           ['id', 'str', 'UNIQUE NOT NULL'],
           ['name', 'str', 'UNIQUE NOT NULL'],
           ['type', 'str', 'NOT NULL'], #tablesync - cluster
           ['status', 'str'], # QUEUED, RUNNING, WAITING
           ['node', 'str'],   # Node which is driving job
           ['reservation', 'str'], # Which Worker is running Job
           ['action', 'str'], # What action is called by job
           ['config', 'str'], # What config is used by action
           ['start_time', 'str'],
           ['next_run_time', 'str'],
           ['last_error', 'str']
       ],
       'id',
       cache_enabled=True
    )
    return # Enter db.create_table statement here
            