FROM joshjamison/uwsgi_python37

ENV PATH="$V_ENV/bin:$PATH"

# Add Dependencies
#COPY /requirements.txt .

RUN apt-get install -y curl

RUN git clone https://github.com/codemation/pyql-cluster.git

RUN pip install -r pyql-cluster/requirements.txt

WORKDIR /pyql-cluster/pyql-cluster/

RUN cp /pyql-cluster/pyql-cluster/sites-available-pyql-cluster /etc/nginx/sites-available/ && \
    ln -s /etc/nginx/sites-available/sites-available-pyql-cluster /etc/nginx/sites-enabled

ENV PYQL_VOLUME_PATH=/mnt/pyql-cluster

EXPOSE 80

CMD ["./startserver.sh"]