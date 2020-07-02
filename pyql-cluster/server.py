from flask import Flask
import os, socket
app = Flask(__name__)
def main(PORT, debug):
    import setup
    setup.run(app)
    app.run('0.0.0.0', PORT, debug=debug)
if __name__ == '__main__':
    try:
        import sys
        print(sys.argv)
        NODE_NAME = sys.argv[1]
        PORT = sys.argv[2]
        CLUSTER_SVC = sys.argv[3]
        CLUSTER_ACTION = sys.argv[4] if len(sys.argv) > 4 else 'join'
        if CLUSTER_ACTION == 'join':
            CLUSTER_TOKEN = sys.argv[5] if len(sys.argv) > 5 else ''
        if '--debug' in sys.argv:
            os.environ['PYQL_DEBUG'] = 'True'
            print(f"pyql-cluster is running in debugger mode")
    except Exception as e:

        print(f"{repr(e)} - expected input: ")
        print("python server.py <node-ip> <node-port> <clusterIp:port> init|join")
    if not PORT == None:
        os.environ['PYQL_NODE'] = NODE_NAME
        os.environ['PYQL_PORT'] = PORT
        os.environ['PYQL_CLUSTER_SVC'] = CLUSTER_SVC
        os.environ['PYQL_CLUSTER_ACTION'] = CLUSTER_ACTION
        if CLUSTER_ACTION == 'join':
            os.environ['']
        main(PORT, debug=True if os.environ.get('PYQL_DEBUG') == 'True'else False)
else:
    # For loading when triggered by uWSGI
    if os.environ['PYQL_TYPE'] == 'K8S' or os.environ['PYQL_TYPE'] == 'DOCKER':
        os.environ['PYQL_NODE'] = socket.gethostbyname(socket.getfqdn())

    import setup
    setup.run(app)