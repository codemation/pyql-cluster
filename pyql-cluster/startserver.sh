#/pyql-cluster/bin/python3.7
echo "starting uvicorn using port "$PYQL_PORT
uvicorn --host 0.0.0.0 --port $PYQL_PORT server:app