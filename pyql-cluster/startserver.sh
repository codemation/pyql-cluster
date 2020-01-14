#!/bin/bash
HOSTIP=$(cat /etc/hosts | grep $(hostname) | awk '{print $1}')
sed -i 's/HOSTIP/'$HOSTIP'/g' /etc/nginx/sites-enabled/sites-available-pyql-cluster
service nginx start
/opt/venv/bin/uwsgi --ini wsgi.ini