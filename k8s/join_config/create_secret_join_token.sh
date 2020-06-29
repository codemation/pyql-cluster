#!/bin/bash
# ./create_secret_join_token.sh <pyql-cluster join-token>
echo "creating secret with token "$1
token=$(echo -n $1 | base64 | tr -d '\n')
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: pyql-cluster-join-token
type: Opaque
data:
  PYQL_CLUSTER_JOIN_TOKEN: $token
EOF