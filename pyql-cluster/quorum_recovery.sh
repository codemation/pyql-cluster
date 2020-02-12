#!/bin/bash
#data='{"set":{"path":"'$2':80"},"where":{"uuid":"'$3'"}}'
curl --request POST http://localhost$1 --header 'Content-type:application/json' --data $2