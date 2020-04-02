def run(server):
    import os
    import subprocess
    print("starting health event checker")
    with open('.cmddir', 'r') as c:
        events = f'{[l for l in c][0]}events/'
        path = f'{events}health/'
    subprocess.Popen(['python', f'{path}checker.py', '/internal/db/check', '30.0', '/internal/db/attach', events])
