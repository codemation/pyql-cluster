#!/bin/bash
docker container rm $(docker container stop $1)
docker container run -d -p $3:80 --name $1 -e 'PYQL_NODE='$2 -e 'PYQL_PORT=80' -e 'PYQL_CLUSTER_SVC='$4':'$3 -e 'PYQL_CLUSTER_ACTION='$5 pyql-cluster
