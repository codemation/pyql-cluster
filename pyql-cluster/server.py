from flask import Flask
import os, socket
app = Flask(__name__)
def main(port, debug):
    import setup
    setup.run(app)
    app.run('0.0.0.0', port, debug=debug)
if __name__ == '__main__':
    try:
        import sys
        print(sys.argv)
        nodeName = sys.argv[1]
        port = sys.argv[2]
        cluster = sys.argv[3]
        clusterAction = sys.argv[4] if len(sys.argv) > 4 else 'join'
        if clusterAction == 'join':
            clusterToken = sys.argv[5] if len(sys.argv) > 5 else ''
        if '--debug' in sys.argv:
            os.environ['PYQL_DEBUG'] = 'True'
            print(f"pyql-cluster is running in debugger mode")
    except Exception as e:

        print(f"{repr(e)} - expected input: ")
        print("python server.py <node-ip> <node-port> <clusterIp:port> init|join")
    if not port == None:
        os.environ['PYQL_NODE'] = nodeName
        os.environ['PYQL_PORT'] = port
        os.environ['PYQL_CLUSTER_SVC'] = cluster
        os.environ['PYQL_CLUSTER_ACTION'] = clusterAction
        if clusterAction == 'join':
            os.environ['']
        main(port, debug=True if os.environ.get('PYQL_DEBUG') == 'True'else False)
else:
    # For loading when triggered by uWSGI
    if os.environ['PYQL_TYPE'] == 'K8S' or os.environ['PYQL_TYPE'] == 'DOCKER':
        os.environ['PYQL_NODE'] = socket.gethostbyname(socket.getfqdn())

    import setup
    setup.run(app)