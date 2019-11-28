def run(server):
    server.cron = {}

    import os
    import subprocess, time
    print("starting job workers")
    with open('.cmddir', 'r') as c:
        path = f'{([l for l in c][0])}events/cluster/'
        print(path)
    subprocess.Popen(['python', f'{path}worker.py', '/internal/job', '5.0'])
    /clusters/updateAll
    # Every 30 seconds refresh state on each cluster node
    subprocess.Popen(['python', f'{path}worker.py', '/internal/job', '5.0'])

    # Update state job - run every 30 seconds
    server.cron['updateState'] = {
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