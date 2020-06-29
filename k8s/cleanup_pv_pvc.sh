#!/bin/bash
for pvc in $(kubectl get pvc | grep pyql | awk '{print $1}'); do kubectl delete pvc $pvc; done
for pv in $(kubectl get pv | grep pyql | awk '{print $1}'); do kubectl delete pv $pv; done