#!/bin/bash
for i in 0 1 2; do kubectl delete pvc pyql-cluster-db-pyql-cluster-$i; done
for i in 0 1 2; do kubectl delete pv pyql-pv-volume-0$i; done
