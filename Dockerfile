FROM joshjamison/uwsgi_python37

ENV PATH="$V_ENV/bin:$PATH"

# Add Dependencies
COPY /requirements.txt .

RUN pip install -r requirements.txt

RUN git clone https://github.com/codemation/pyql-cluster.git

WORKDIR /pyql-cluster/pyql-cluster/

RUN cp /pyql-cluster/pyql-cluster/sites-available-pyql-cluster /etc/nginx/sites-available/ && \
    ln -s /etc/nginx/sites-available/sites-available-pyql-cluster /etc/nginx/sites-enabled

EXPOSE 80

CMD ["./startserver.sh"]