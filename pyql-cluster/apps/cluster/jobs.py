async def run(server):
    import os, uuid
    import asyncio
    from fastapi import Request, Depends

    log = server.log

    async def wait_on_jobs(pyql, cur_ind, job_list, waiting_on=None):
        """
            job queing helper function - guarantees 1 job runs after the other by creating "waiting jobs" 
             dependent on the first job completing
        """
        if len(job_list) > cur_ind + 1:
            job_list[cur_ind]['config']['nextJob'] = await wait_on_jobs(pyql, cur_ind+1, job_list)
        if cur_ind == 0:
            result = await jobs_add(job_list[cur_ind])
            return result['job_id']
        result = await jobs_add(job_list[cur_ind], status='waiting')
        return result['job_id']

    server.wait_on_jobs = wait_on_jobs

    @server.trace
    async def re_queue_job(job, **kw):
        await job_update(job['type'], job['id'],'queued', {"message": "job was requeued"}, **kw)

    @server.api_route('/cluster/pyql/jobmgr/cleanup', methods=['POST'])
    async def cluster_jobmgr_cleanup_api(
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_jobmgr_cleanup( request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_jobmgr_cleanup(**kw):
        return await jobmgr_cleanup(**kw)
    @server.trace
    async def jobmgr_cleanup(**kw):
        """
            invoked on-demand or by cron to check for stale jobs & requeue
        """ 
        trace=kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        jobs = await server.cluster_table_select(pyql, 'jobs', method='GET', **kw)
        jobs = jobs['data']
        for job in jobs:
            if not job['next_run_time'] == None:
                # Cron Jobs 
                if time.time() - float(job['next_run_time']) > 240.0:
                    if not job['node'] == None:
                        await re_queue_job(job, trace=kw['trace'])
                        continue
                    else:
                        trace.error(f"job {job['id']} next_run_time is set but stuck for an un-known reason")
            if not job['start_time'] == None:
                time_running = time.time() - float(job['start_time'])
                if time_running > 240.0:
                    # job has been running for more than 4 minutes
                    trace.warning(f"job {job['id']} has been {job['status']} for more than {time_running} seconds - requeuing")
                    await re_queue_job(job, trace=kw['trace'])
                if job['status'] == 'queued':
                    if time_running > 30.0:
                        trace.warning(f"job {job['id']} has been queued for more {time_running} seconds - requeuing")
                        await re_queue_job(job, trace=kw['trace'])
            else:
                if job['status'] == 'queued':
                    # add start_time to check if job is stuck
                    await cluster_table_change(
                        pyql, 'jobs', 'update', 
                        {'set': {
                            'start_time': time.time()}, 
                        'where': {
                            'id': job['id']}}, 
                        **kw)
            if job['status'] == 'waiting':
                waiting_on = None
                for jb in jobs:
                    if 'nextJob' in jb['config']:
                        if jb['config']['nextJob'] == job['id']:
                            waiting_on = jb['id']
                            break
                if waiting_on == None:
                    trace.warning(f"Job {job['name']} was waiting on another job which did not correctly queue, queuing now.")
                    await re_queue_job(job, **kw)
                    
        return {"message": trace.warning(f"job manager cleanup completed")}
    server.clusterjobs['jobmgr_cleanup'] = jobmgr_cleanup

    @server.api_route('/cluster/jobqueue/{job_type}', methods=['POST'])
    async def cluster_jobqueue(
        job_type: str, 
        request: Request,
        token: dict = Depends(server.verify_token)
    ):
        return await cluster_jobqueue(job_type,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_jobqueue(job_type, **kw):
        return await jobqueue(job_type, **kw)
    

    @server.trace
    async def jobqueue_reserve_job(job, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        trace = kw['trace']
        reservation = trace.get_root_operation()

        async def reserve_or_rollback(rollback=False):

            job_update = {
                'set': {
                    'node': server.PYQL_NODE_ID if not rollback else None,
                    'reservation': reservation if not rollback else None
                }, 
                'where': {
                    'id': job['id'], 
                    'node': None if not rollback else server.PYQL_NODE_ID
                }
            }
            if rollback:
                job_update['reservation'] = reservation

            result = await cluster_table_change(pyql, 'jobs', 'update', job_update, **kw)
            op = 'reserve' if not rollback else 'rollback'
            return trace(f"{op} completed for job {job['id']} with result {result}")
        _ = await reserve_or_rollback()

        # verify if job was reserved by node and pull config
        job_select = {
            'select': ['*'],
            'where': {
                'id': job['id']
            }
        }
        start_time, max_timeout = time.time(), 20.0
        while time.time() - start_time < max_timeout:
            job_check = await server.cluster_table_select(pyql, 'jobs', data=job_select, method='POST', **kw)
            if len(job_check['data']) == 0:
                return {"message": trace(f"failed to reserve job {job['id']}, no longer exists")}
            trace(f"job_check: {job_check}")
            job_check = job_check['data'][0]
            if job_check['node'] == None:
                # wait until 'node' is assigned
                await asyncio.sleep(0.1)
                continue
            if (job_check['node'] == server.PYQL_NODE_ID and 
                job_check['reservation'] == reservation ):
                return job_check
            await reserve_or_rollback(rollback=True)
            return {"message": trace(f"{job['id']} was reserved by another worker")}
        else:
            _ = await reserve_or_rollback(rollback=True)
            return {"message": trace.error(f"timeout of {max_timeout} reached while trying to reserve job")}

    @server.trace
    async def jobqueue(job_type, node=None, **kw):
        """
            Used by jobworkers or tablesyncers to pull jobs from clusters job queues
            job_type = 'job|syncjob|cron'
        """
        trace = kw['trace']
        queue = f'{job_type}s' if not job_type == 'cron' else job_type

        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        ready = await server.data['cluster'].tables['quorum'].select('ready', where={'node': server.PYQL_NODE_ID})
        ready  = ready[0]['ready']
        if pyql == None or not ready:
            return {"message": trace("cluster is not ready or still bootstrapping, try again later")}

        endpoints = await server.data['cluster'].tables['endpoints'].select(
            '*'
        )

        if len(endpoints) == 0:
            return {"message": trace("cluster is bootstrapped, but still syncing")}

        quorum = await server.cluster_quorum_query()

        node = server.PYQL_NODE_ID

        trace(f"checking for {job_type} jobs to run")

        job_select = {
            'select': ['id', 'name', 'type', 'next_run_time', 'node'], 
            'where':{
                'status': 'queued',
                'type': queue
            }
        }
        if not job_type == 'cron':
            job_select['where']['node'] = None
        trace("starting to pull list of jobs")
        job_list = await server.cluster_table_select(
            pyql, 'jobs', data=job_select, method='POST', quorum=quorum, **kw)
        if not job_list:
            return {"message": trace("unable to pull jobs at this time")}
        trace(f"type {job_type} - finished pulling list of jobs - job_list {job_list} ")
        job_list = job_list['data']
        for i, job in enumerate(job_list):
            if not job['next_run_time'] == None:
                #Removes queued job from list if next_run_time is still in future 
                if float(job['next_run_time']) < time.time():
                    job_list.pop(i)
                if not job['node'] == None:
                    if time.time() - float(job['next_run_time']) > 120.0:
                        trace.error(f"found stuck job {job['id']} assigned to node {job['node']} - begin re_queue job")
                        await re_queue_job(job, trace=kw['trace'])
                        trace(f"found stuck job assigned to node {job['node']} - finished re_queue job")

        if len(job_list) == 0:
            return {"message": trace("no jobs to process at this time")}
        if job_type == 'cron':
            job_list = sorted(job_list, key=lambda job: job['next_run_time'])
            job = job_list[0]
        else:
            latest = 3 if len(job_list) >= 3 else len(job_list)
            job_index = randrange(latest-1) if latest -1 > 0 else 0
            job = job_list[job_index]
        
        job_select['where']['id'] = job['id']

        trace.warning(f"Attempt to reserve job {job} if no other node has taken ")

        # reserve job
        return await jobqueue_reserve_job(job, **kw)

    @server.api_route('/cluster/job/{job_type}/{job_id}/{status}', methods=['POST'])
    async def cluster_job_update_api(
        job_type: str, 
        job_id: str, 
        status: str, 
        request: Request, 
        job_info: dict = None,
        token: dict = Depends(server.verify_token),
    ):
        return await cluster_job_update(job_type, job_id, status, job_info=job_info,  request=await server.process_request(request))

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_job_update(job_type, job_id, status, **kw):
        return await job_update(job_type, job_id, status, **kw)

    async def job_update(job_type, job_id, status, job_info={}, **kw):
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        trace=kw['trace']
        if status == 'finished':
            update_from = {'where': {'id': job_id}}
            if job_type == 'cron':
                cron_select = {'select': ['id', 'config'], 'where': {'id': job_id}}
                job = await server.cluster_table_select(pyql, 'jobs', data=cron_select, method='POST', **kw)
                job = job['data'][0]
                update_from['set'] = {
                    'node': None, 
                    'status': 'queued',
                    'start_time': None}
                if job:
                    update_from['set']['next_run_time'] = str(time.time()+ job['config']['interval'])
                else:
                    update_from['set']['next_run_time'] = str(time.time() + 25.0)
                    trace.error(f"error pulling cron job {job_id} - proceeding to mark finished")
                return await cluster_table_change(pyql, 'jobs', 'update', update_from, **kw) 
            return await cluster_table_change(pyql, 'jobs', 'delete', update_from, **kw)
        if status == 'running' or status == 'queued':
            update_set = {'last_error': {}, 'status': status}
            for k,v in job_info.items():
                if k =='start_time' or k == 'status':
                    update_set[k] = v
                    continue
                update_set['last_error'][k] = v
            update_where = {'set': update_set, 'where': {'id': job_id}}
            if status =='queued':
                update_where['set']['node'] = None
                update_where['set']['start_time'] = None
                if job_type == 'cron':
                    update_where['set']['next_run_time'] = str(time.time())
            else:
                update_where['set']['start_time'] = str(time.time())
            return await cluster_table_change(pyql, 'jobs', 'update', update_where, **kw)

    @server.api_route('/cluster/{job_type}/add', methods=['POST'])
    async def cluster_jobs_add(job_type: str, config: dict, request: Request, token: dict = Depends(server.verify_token)):
        return await cluster_jobs_add(
            job_type, 
            config, 
            request=await server.process_request(request)
        )

    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_jobs_add(job_type, config, **kw):
        return await jobs_add(config=config, **kw)
        
    @server.trace
    async def jobs_add(config=None, **kw):
        """
        meant to be used by node workers which will load jobs into cluster job queue
        to avoiding delays from locking during change operations
        For Example:
        # Load a job into node job queue
        server.jobs.append({'job': 'job-name', ...})
        Or
        cluster_jobs_add('syncjobs', jobconfig, status='WAITING')

        """
        pyql = await server.env['PYQL_UUID'] if not 'pyql' in kw else kw['pyql']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        trace=kw['trace']
        trace(f"called with config: {config} - {kw}")

        job = config
        trace.warning(f"jobs_add for job {job} started")
        job_id = f'{uuid.uuid1()}'
        job_type = job['job_type']
        job_insert = {
            'id': job_id,
            'name': job['job'],
            'type': job_type if not job_type == 'cluster' else 'jobs', # cluster is converted to jobs
            'status': 'queued' if not 'status' in kw else kw['status'],
            'action': job['action'],
            'config': job['config']
        }
        if job_type == 'cron':
            job_insert['next_run_time'] = str(float(time.time()) + job['config']['interval'])
        else:
            job_check = await server.cluster_table_select(
                pyql, 'jobs', 
                data={'select': ['id'], 'where': {'name': job['job']}},
                method='POST',
                **kw
                )
            if not job_check:
                return {"message": trace(
                            f'job {job} not added, could not verify if job exists in table, try again later'
                        )
                    }
            job_check= job_check['data']

            if len(job_check) > 0:
                job_status = f"job {job_check[0]['id'] }with name {job['job']} already exists"
                return {
                    'message': trace.warning(job_status),
                    'job_id': job_check[0]['id']
                }

        response = await cluster_table_change(pyql, 'jobs', 'insert', job_insert, **kw)
        trace.warning(f"cluster {job_type} add for job {job} finished - {response}")
        return {
            "message": f"job {job} added to jobs queue - {response}",
            "job_id": job_id}
    server.clusterjobs['jobs_add'] = jobs_add
    

    @server.api_route('/cluster/jobs/{jobtype}/run', methods=['POST'])
    async def cluster_job_check_and_run_api(jobtype: str, request: Request, token: dict = Depends(server.verify_token)):
        log.info(jobtype)
        return await cluster_job_check_and_run(
            jobtype, 
            request=await server.process_request(request)
        )
    @server.state_and_quorum_check
    @server.is_authenticated('pyql')
    @server.trace
    async def cluster_job_check_and_run(job_type, **kw):
        return await job_check_and_run(job_type, **kw)

    @server.trace
    async def job_check_and_run(job_type, **kw):
        trace = kw['trace']
        kw['loop'] = asyncio.get_running_loop() if not 'loop' in kw else kw['loop']
        # try to pull job 
        job = await jobqueue(job_type, server.PYQL_NODE_ID, **kw)
        if not job or 'message' in job:
            trace(f"{job_type} - {job}")
            return job
        trace(f"{job_type} - job pulled {job['name']}")
        job_config = job['config']
        try:
            await job_update(
                job_type, 
                job['id'], 
                'running', 
                job_info={"message": f"starting {job['name']}"}, 
                **kw
                )
        except Exception as e:
            trace.exception(f"exception while marking job {job['name']} running")

        trace(f"running job with config {job_config}")
        result, error = None, None
        try:
            result = await server.clusterjobs[job['action']](config=job_config, **kw)
        except Exception as e:
            error = trace.exception(f"exception while running job {job['name']}")

        if result:
            await job_update(
                job_type, job['id'], 'finished', 
                job_info={
                    "message": f"finished {job['name']}",
                    "result": result
                    }, **kw)
        else:
            error = trace(f"Error while running job - {error}")
            await job_update(job_type, job['id'], 'queued', job_info={"error": f"{error} - requeuing"}, **kw)
            return {"error": error}
        if 'nextJob' in job_config:
            await job_update(job_type, job_config['nextJob'], 'queued', job_info={"message": f"queued after {job['name']} completed"}, **kw)
        trace(f"finished {job['name']} with result: {result}")
        return {"result": result}
        
    server.job_check_and_run = job_check_and_run
    
    @server.on_event('startup')
    async def jobs_startup():
        if await server.env['SETUP_ID'] == server.setup_id:

            if os.environ['PYQL_CLUSTER_ACTION'] == 'init':
                #Job to trigger cluster_quorum()
                """
                init_quorum = {
                    "job": "init_quorum",
                    "job_type": "cluster",
                    "method": "POST",
                    "action": 'cluster_quorum_update',
                    "path": "/pyql/quorum",
                    "config": {}
                }
                """
                init_mark_ready_job = {
                    "job": "init_mark_ready",
                    "job_type": "cluster",
                    "method": "POST",
                    "path": "/cluster/pyql/ready",
                    "config": {'ready': True}
                }
                # Create Cron Jobs inside init node
                cron_jobs = []
                if not os.environ.get('PYQL_TYPE') == 'K8S':
                    #await server.internal_job_add(init_quorum)
                    #server.internal_job_add(initMarkReadyJob)
                    cron_jobs.append({
                        'job': 'cluster_quorum_check',
                        'job_type': 'cron',
                        "action": 'cluster_quorum_check',
                        "config": {"interval": 15}
                    })
                for i in [30,90]:
                    cron_jobs.append({
                        'job': f'tablesync_check_{i}',
                        'job_type': 'cron',
                        "action": 'tablesync_mgr',
                        "config": {"interval": i}
                    })
                    cron_jobs.append({
                        'job': f'cluster_job_cleanup_{i}',
                        'job_type': 'cron',
                        'action': 'jobmgr_cleanup',
                        'config': {'interval': i}
                    })
                for job in cron_jobs:
                    new_cron_job = {
                        "job": f"add_cron_job_{job['job']}",
                        "job_type": 'cluster',
                        "action": "jobs_add",
                        "config": job,
                    }
                    log.warning(f"adding job {job['job']} to internaljobs queue")
                    await server.internal_job_add(new_cron_job)