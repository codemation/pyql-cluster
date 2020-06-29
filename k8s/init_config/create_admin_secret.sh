#!/bin/bash
# ./create_secret_join_token.sh <pyql-cluster join-token>
encodedpw=$(echo -n $1 | base64 | tr -d '\n')
echo "creating secret with encoded password "$encodedpw
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: pyql-cluster-init-auth
type: Opaque
data:
  PYQL_CLUSTER_INIT_ADMIN_PW: $encodedpw
EOF