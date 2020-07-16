#!/bin/bash
# Used to BUILD docker image & start a pyql-cluster instance for testing
#usage
# ./restart/pyql_cluster.sh <tag> <local-port> <cluster host> <cluster-port> <init|join> [token]
#Example:
# ./restart_pyql_cluster.sh dryrun.0.0 8090 192.168.3.33 8090 init
# build image

action=$(echo $5 | grep 'join' > /dev/null && echo -n 'join' || echo -n 'init')
env0='-e PYQL_CLUSTER_SVC='$3':'$4' -e PYQL_PORT='$2' -e PYQL_CLUSTER_ACTION='$action' -e PYQL_TYPE=DOCKER '
env1='-e PYQL_HOST='$3

echo $5 | grep 'join' > /dev/null
if [ $? -eq 0 ]
then
    # join using token
    env0=$env0' -e PYQL_CLUSTER_JOIN_TOKEN='$6
    docker build docker_pyql-cluster/ -t joshjamison/pyql-cluster:$1 $7
    echo $8 | grep 'debug' && env0=$env0' -e PYQL_DEBUG=True'
else 
    # init
    docker build docker_pyql-cluster/ -t joshjamison/pyql-cluster:$1 $6
    echo $7 | grep 'debug' && env0=$env0' -e PYQL_DEBUG=True' && echo '## DEBUGGING ENABLED ##'
fi

# remove old pyql-cluster-instance container if running 

docker container rm $(docker container stop pyql-cluster-$2) || docker container rm pyql-cluster-$2

# remove old volume if not rejoin

echo $5 | grep 'rejoin' > /dev/null 
if [ $? -eq 0 ]
then
    echo "rejoin called - will try using existing volume path"
else
    echo 'cleaning up existing volume path'
    sudo rm -rf $(pwd)/pyql-cluster-$2-vol/

    # check & cleanup 
    ls $(pwd)/pyql-cluster-$2-vol/ && sudo rm -rf $(pwd)/pyql-cluster-$2-vol/ || echo "cleaned up existing pyql-cluster volume"

    # creat new volume dir
    mkdir $(pwd)/pyql-cluster-$2-vol
fi

# starting pyql-cluster instance

docker container run --name pyql-cluster-$2 $env0 $env1 -p $2:8090 -v $(pwd)/pyql-cluster-$2-vol:/mnt/pyql-cluster -d joshjamison/pyql-cluster:$1