def run(server):
    server.cron = {}
    log = server.log
    import os
    import subprocess, time, requests,json
    print("starting job workers")
    with open('.cmddir', 'r') as c:
        events = f'{[l for l in c][0]}events/'
        path = f'{events}cluster/'

    # Create Job workers to pull & perform work on cluster jobs
    log.info(f'jobworker to pull jobs from cluster job queue via /cluster/jobqueue/job')
    #for _ in range(2):
    subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobqueue/job', '10.0', events])

    # Create Table Sync Workers
    subprocess.Popen(['python', f'{path}tablesyncer.py', '/cluster/jobqueue/syncjob', '10.0', events])

    # Create Cron Job Workers
    subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobqueue/cron', '15.0', events])