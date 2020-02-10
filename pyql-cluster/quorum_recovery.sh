#!/bin/bash
data='{"set": {"path": "'$1':80"}, "where": {"uuid": "'$2'"}}'
curl --request POST http://localhost/db/cluster/table/endpoints/update --header 'Content-type:application/json' --data $data