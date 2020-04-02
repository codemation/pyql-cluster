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
echo $PYQL_HOST | grep $PYQL_HOST
if [ $? -eq 0 ]
then
    # used by stand-alone / test pyql-cluster instances with a variable hostname 
    sed -i 's/PYQL_HOST/'$PYQL_HOST'/g' /etc/nginx/sites-enabled/sites-available-pyql-cluster
fi

service nginx start
/opt/venv/bin/uwsgi --ini wsgi.ini