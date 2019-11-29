def run(server):
    server.cron = {}

    import os
    import subprocess, time, requests
    print("starting job workers")
    with open('.cmddir', 'r') as c:
        path = f'{([l for l in c][0])}events/cluster/'
        print(path)

    # Create Job workers to pull & perform work on cluster jobs
    for _ in range(2):
        subprocess.Popen(['python', f'{path}jobworker.py', '/cluster/jobqueue/job', '5.0'])

    # Update state job - run every 30 seconds
    updateStateCron = {
        'name': 'updateStateCron'
        'job': {
            'job': 'updateState',
            'jobType': 'cluster',
            'method': 'POST'
            'path': '/clusters/updateAll'
        },
        'interval': 30.0,
        'lastRunTime': time.time(),
        'status': 'queued'
    }
    # Add Cron job to /clusters/cron/job/add
    r = request.post('https://localhost:8080/clusters/cron/job/add', data=updateStateCron)

    # Worker to trigger refresh of cronjob status - if interval is reached server will create a job
    subprocess.Popen(['python', f'{path}cron.py', '/clusters/cron/job', '16.0'])

    # Create Table Sync Workers
    subprocess.Popen(['python', f'{path}tablesyncer.py', '/cluster/jobqueue/syncjobs', '30.0'])
    
