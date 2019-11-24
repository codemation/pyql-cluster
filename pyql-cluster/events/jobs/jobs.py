def run(server):
    import os
    import subprocess
    print("starting job workers")
    with open('.cmddir', 'r') as c:
        path = f'{([l for l in c][0])}events/jobs/'
        print(path)
    subprocess.Popen(['python', f'{path}worker.py', '/internal/job', '5.0'])