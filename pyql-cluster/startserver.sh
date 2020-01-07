#!/bin/bash
service nginx start
/opt/venv/bin/uwsgi --ini wsgi.ini