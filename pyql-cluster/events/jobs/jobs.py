async def run(server):
    import os, asyncio
    asyncio.set_event_loop(server.event_loop)
    log = server.log

    NODE_IP = os.environ.get('PYQL_NODE')

    if os.environ.get('PYQL_TYPE') in ['K8S', 'DOCKER']:
        import socket
        NODE_IP = socket.gethostbyname(socket.getfqdn())

    CLUSTER_SVC_NAME = f'http://{os.environ["PYQL_CLUSTER_SVC"]}'
    NODE_PATH = f'http://{NODE_IP}:{os.environ["PYQL_PORT"]}'
    

    async def get_and_process_job():
        job = await server.job_queue_pull()
        if 'message' in job:
            return log.warning(f"get_and_process_job - result {job}")

        log.warning(f"get_and_process_job pulled job {job}"
        job_id = job['id']
        job = job['config']
        try:
            if job['job_type'] == 'cluster':
                #Distribute to cluster job queue
                if 'join_cluster' in job['job']: # need to use join_token
                    log.warning(f"join cluster job {job}, attempting to join")
                    message, rc = await server.probe(
                        f"{CLUSTER_SVC_NAME}{job['path']}", 
                        method=job['method'], 
                        data=job['data'], 
                        token=job['join_token'], 
                        timeout=30)
                else:
                    log.warning(f"adding job {job} to cluster jobs queue")
                    message = await server.clusterjobs['jobs_add'](job)
                    #message, rc = probe(f"{CLUSTER_SVC_NAME}/cluster/jobs/add", 'POST', job, timeout=30)
                log.warning(f"finished adding job {job} to cluster jobs queue {message}")
            if job['job_type'] == 'node':
                auth = 'local' if not 'init_cluster' in job['job'] else 'cluster'
                if 'init_cluster' in job['job']:
                    message = await server.join_cluster('pyql', job['data'])
                else:
                    message, rc = await server.probe(
                        f"{NODE_PATH}{job['path']}", 
                        method=job['method'], 
                        data=job['data'], auth=auth
                        )
            
            if message:
                try:
                    result = await server.job_queue_action(job_id, 'finished')
                    return message
                    #probe(f'{NODE_PATH}/internal/job/{job_id}/finished', 'POST', auth='local', session=session)
                except Exception as e:
                    message = log.exception(f'encountered exception finishing job, need to cleanup {job_id} later')
            else:
                log.warning(f"{job['job']} completed with no message")
        except Exception as e:
            message = log.exception(f"{os.environ['HOSTNAME']} exception hanlding job {job} - add back to queue")
            #probe(f'{NODE_PATH}/internal/job/{job_id}/queued', 'POST', auth='local', session=session)
        await server.job_queue_action(job_id, 'queued')
        return message

    async def server_get_and_process_job():
        return await get_and_process_job()
    server.tasks.append(
        server_get_and_process_job
    )