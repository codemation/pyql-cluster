# internal
async def run(server):
    import uuid
    log = server.log
    from fastapi import Request

    async def db_check(database):
        db = server.data[database]
        if db.type == 'sqlite':
            result = await db.get(f"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = [t[0] for t in result]
            if database == 'cluster':
                cluster_tables = ['clusters', 'endpoints', 'tables', 'state', 'pyql']
                for index, check in enumerate(cluster_tables):
                    log.info(f"checking {check}")
                    if not check in tables:
                        error = f"missing table {check} in database {database}"
                        server.http_exception(500, log.error(error))
                    
            for r in result:
                log.info(f"db_check - found {r}")
        else:    
            tables = await db.run('show tables')
            log.info(f"db_check result: {tables}")
            for table in tables:
                if not table[0] in server.data[database].tables:
                    await server.data[database].load_tables()
        return {"messages": f"{database} status ok", "tables": list(server.data[database].tables.keys())}
    server.db_check = db_check
    
    async def internal_job_add(job):
        job_id = str(uuid.uuid1())
        await server.clusters.internaljobs.insert(**{
            'id': job_id,
            'name': job['job'],
            'status': 'queued',
            'config': job
        })
    server.internal_job_add = internal_job_add

    @server.api_route('/internal/job/{id}/{action}', methods=['POST'])
    async def internal_job_queue_action(id: str, action: str, request: Request):
        return await internal_job_queue_action(id, action,  request=await server.process_request(request))
    @server.is_authenticated('local')
    async def internal_job_queue_action(id, action, **kw):
        return await job_queue_action(id, action, **kw)

    async def job_queue_action(job_id, action, **kw):
        log.warning(f"job_queue_action {job_id} - {action}")
        try:
            if action == 'finished':
                result = await server.clusters.internaljobs.delete(where={'id': job_id})
                log.warning(f"finished result {result}")
            if action == 'queued':
                await server.clusters.internaljobs.update(status='queued', where={'id': job_id})
            return {"message": f"{action} on {job_id} completed successfully"}
        except Exception as e:
            return {
                "message": log.exception(f"error when performing {action} on job_id {job_id}")
                }
    server.job_queue_action = job_queue_action

    @server.api_route('/internal/job')
    async def internal_job_queue_pull_api(request: Request):
        return await internal_job_queue_pull( request=await server.process_request(request))

    @server.is_authenticated('local')
    async def internal_job_queue_pull(**kw):
        return await job_queue_pull(**kw)
    async def job_queue_pull(**kw):
        jobs = await server.clusters.internaljobs.select('id', where={'status': 'queued'})
        if len(jobs) > 0:
            for job in jobs:
                await server.clusters.internaljobs.update(status='running', where={'id': job['id'], 'status': 'queued'})
                reserved = await server.clusters.internaljobs.select('*', where={'id': job['id'], 'status': 'running'})
                log.warning(f"job_queue_pull reserved job: {reserved}")
                if len(reserved) == 1:
                    reserved_config = reserved[0]['config']
                    return {'id': job['id'], 'config': reserved_config}
        return {"message": "no jobs in queue"}
    
    server.job_queue_pull = job_queue_pull

    @server.api_route('/internal/jobs')
    async def internal_list_job_queue(request: Request):
        return await internal_list_job_queue( request=await server.process_request(request))
    @server.is_authenticated('local')
    async def internal_list_job_queue(**kw):
        return {'jobs': server.jobs}

    @server.api_route('/internal/db/check')
    async def internal_db_check_api(request: Request):
        return await internal_db_check_auth( request=await server.process_request(request))

    @server.is_authenticated('local')
    async def internal_db_check_auth(**kw):
        return await internal_db_check(**kw)
    async def internal_db_check(**kw):
        messages = []
        for database in server.data:
            messages.append(await db_check(database))
        return {"result": messages if len(messages) > 0 else "No databases attached", "jobs": server.jobs}
    
    server.internal_db_check = internal_db_check

    @server.api_route('/internal/db/{database}/status')
    async def internal_db_status_api(database: str, request: Request):
        return await internal_db_status(database,  request=await server.process_request(request))

    @server.is_authenticated('local')
    async def internal_db_status(database, **kw):
        if database in server.data:
            return await db_check(database)
        else:
            server.http_exception(404, f"database with name {database} not found")