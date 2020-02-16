#!/bin/bash
while true;
do
    HOSTIP=$(cat /etc/hosts | grep $(hostname) | awk '{print $1}')
    sed -i 's/HOSTIP/'$HOSTIP'/g' /etc/nginx/sites-enabled/sites-available-pyql-cluster
    grep $HOSTIP /etc/nginx/sites-enabled/sites-available-pyql-cluster
    if [ $? == 0 ]
    then
        break
    fi
done
service nginx start
/opt/venv/bin/uwsgi --ini wsgi.ini