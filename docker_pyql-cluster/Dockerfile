FROM joshjamison/python37:latest

RUN apt-get update && apt-get install -y curl

RUN git clone --single-branch --branch fastapi https://github.com/codemation/pyql-cluster.git

RUN pip install -r pyql-cluster/requirements.txt

ENV V_ENV=/pyql-cluster

RUN virtualenv -p python3.7  $V_ENV

ENV PATH="$V_ENV/bin:$PATH"

RUN pip install -r pyql-cluster/requirements.txt

WORKDIR /pyql-cluster/pyql-cluster/

ENV PYQL_VOLUME_PATH=/mnt/pyql-cluster

EXPOSE 8090

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "8090", "server:app"]