#!/bin/bash
docker container rm $(docker container stop $1)
docker container run -d -p 8080:80 --name $1 -e 'PYQL_NODE=localhost' -e 'HOSTNAME=localhost' -e 'PYQL_PORT=80' -e 'PYQL_CLUSTER_SVC=localhost' -e 'PYQL_CLUSTER_ACTION=init' pyql-cluster
