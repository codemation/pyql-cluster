def run(server):
    import os
    import subprocess
    log = server.log
    log.info("starting job workers")
    with open('.cmddir', 'r') as c:
        path = f'{([l for l in c][0])}events/jobs/'
    subprocess.Popen(['python', f'{path}worker.py', '/internal/job', '30.0'])