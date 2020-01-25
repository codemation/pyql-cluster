from flask import Flask
import os, socket
app = Flask(__name__)
def main(port):
    import setup
    setup.run(app)
    app.run('0.0.0.0', port, debug=True)
if __name__ == '__main__':
    try:
        import sys
        print(sys.argv)
        nodeName = sys.argv[1]
        port = sys.argv[2]
        cluster = sys.argv[3]
        clusterAction = sys.argv[4] if len(sys.argv) > 4 else 'join'
    except:
        print("expected input: ")
        print("python server.py <node-ip> <node-port> <clusterIp:port> init|join")
    if not port == None:
        os.environ['PYQL_NODE'] = nodeName
        os.environ['PYQL_PORT'] = port
        os.environ['PYQL_CLUSTER_SVC'] = cluster
        os.environ['PYQL_CLUSTER_ACTION'] = clusterAction
        main(port)
else:
    # For loading when triggered by uWSGI
    if os.environ['PYQL_TYPE'] == 'K8S':
        os.environ['PYQL_NODE'] = socket.gethostbyname(socket.getfqdn())

    import setup
    setup.run(app)