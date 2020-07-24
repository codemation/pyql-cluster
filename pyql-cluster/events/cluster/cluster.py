async def run(server):

    def get_factory_coro(job_type):
        async def job_check_and_run():
            await server.job_check_and_run(job_type)
        f = job_check_and_run
        f.__name__ = f'{job_type}_{f}'
        return f

    # cron job run
    server.tasks.append(
        get_factory_coro('cron')
    )
    # sync job run
    server.tasks.append(
        get_factory_coro('syncjob')
    )
    # job run 
    server.tasks.append(
        get_factory_coro('job')
    )


    """
    log = server.log
    import os
    server.cron = {}
    import subprocess, time, requests,json
    print("starting job workers")
    with open('.cmddir', 'r') as c:
        events = f'{[l for l in c][0]}events/'
        path = f'{events}cluster/'

    # Create Job workers to pull & perform work on cluster jobs
    log.info(f'jobworker to pull jobs from cluster job queue via /cluster/jobqueue/job')
    #for _ in range(2):
    #subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobqueue/job', '10.0', events])
    subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobs/job/run', '10.0', events])

    ## Create Table Sync Workers
    #subprocess.Popen(['python', f'{path}tablesyncer.py', '/cluster/jobqueue/syncjob', '10.0', events])
    # Create Table Sync Workers
    subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobs/syncjob/run', '10.0', events])

    # Create Cron Job Workers
    # subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobqueue/cron', '15.0', events])
    # Create Cron Job Workers
    subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobs/cron/run', '15.0', events])
    """