# pyql-cluster
A distributed and scale-in/out REST API application, built with python, which provides users of any language the ability to access, store, and update data, via the Rest PYQL JSON syntax, with a cluster of managed & distributed table replicas consisting of multiple database types & locations.

## Key Features
- Scale-in / out orchestration plane  - As load increases / decreases, endpoints can be scaled up / down. 
- Masterless Scale in / out data table clusters - As load increases, pyql-cluster can add replicas to clusters of tables or decrease on reduced load. Providing read load-balancing, table redundancy, self-healing 
- Data access via REST - access to table data using PYQL JSON query syntax 
- Easy Data sharding via table clusters - data clusters proivde any easy method of creating data table shards with individual scaling capabilities
- Secure/Shareable - Cluster access secured using tokens for Cluster Owners & Sharable for other users 
- No downtime Upgrades - Min 3 node cluster 

## Quick Start - Try it out
requirements: a bootstrapped kubernetes cluster

### Bootstrap pyql-cluster 

        $ git clone https://github.com/codemation/pyql-cluster.git
        $ cd pyql-cluster/k8s/
        $ kubectl apply -f init_config/
        configmap/pyql-cluster-config configured
        persistentvolume/pyql-pv-volume-00 created
        persistentvolume/pyql-pv-volume-01 created
        persistentvolume/pyql-pv-volume-02 created
        secret/pyql-cluster-init-auth created 

        # Default secret password is 'abcd1234'

        $ kubectl apply -f init/
        statefulset.apps/pyql-cluster created
        service/pyql-cluster-lb created

        $ kubectl get service pyql-cluster-lb
        NAME              TYPE       CLUSTER-IP      EXTERNAL-IP   PORT(S)        AGE
        pyql-cluster-lb   NodePort   10.97.143.170   <none>        80:32543/TCP   4d13h

### Create a new data user: 

        $ curl -X POST -H "Authentication: Basic $(echo 'admin:abcd1234' | base64)" \
            -H "Content-Type: application/json" --data '{"username":"aNewUser","email":"newUser@aCompany.com","password":"abcd1234"}' \
            http://pyql-cluster:32543/auth/user/register
        {"message":"user created successfully"}

### Create a join token for user

        $ curl -H "Authentication: Basic $(echo 'aNewUser:abcd1234' | base64)" \
            http://pyql-cluster:32543/auth/token/join
        {"join":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6ImU3MjRjODQyLWIzNDQtMTFlYS05NjgxLTYyOGNhNTQ1MTNjYyIsImV4cGlyYXRpb24iOiJqb2luIiwiY3JlYXRlVGltZSI6MTU5MjY5MjE0OC4zNjk1NTE0fQ.NtVVHjfTTuAZXWUqb-RVv2kxAWBpSHZlKUA8Smw1OSk"}

### Deploy a Data Cluster



#### Lifecycle of a data-cluster - IMPORTANT - Read before use!
A data cluster is a cluster of [pyql-rest](https://github.com/codemation/pyql-rest) endpoint tables. This may be all tables, a few or even just one table from a database. 

Considerations:
* When an endpoint joins a cluster, the joining endpoint decides which tables to add to the cluster. If the cluster already contained other tables, these tables will then be added on the new endpoint and the new table will be added on other existing endpoints. 
* Tables already existing in the cluster will take priority over 'added' tables, meaning if a new endpoint trys to add table 'employees' and the able already existed, the newly added endpoints table 'employees' will be dropped & synced with the existing tables. This behavior underscores the importance of knowing what tables already exist within a cluster & configuring separate clusters where conflicts need to be avoided. 
* TO AVOID DATA-LOSS - DO NOT join a cluster with a loaded table if table name already exists in the cluster with different or un-related data. If re-joining, this will cause a resync(only changes will be applied to joining table), if joining for first time, a drop is issued against the table.   
* Cluster names are unique only with the creating user, so the same cluster name can be used by multiple users
* Clusters may contain tables from multiple database types (MySql & Sqlite3 currently supported by pyql-rest endpoints)
* Endpoints may exist in many different locations and in many different forms, and must be accessible by all pyql-cluster endpoints services requests.

### What type of endpoints can join a data cluster?
pyql-cluster does not manage database credentials, but instead manages pyql-rest endpoint tokens for auth to data-cluster tables in pyql-rest endpoints. This means a joining endpoint must be a [pyql-rest](https://github.com/codemation/pyql-rest) endpoint, the method a pyql-rest endpoint joins a data-cluster can vary:
* Endpoints of type PYQL_TYPE=K8S expect to join a pyql-cluster immedietly and are configured with parameters to connect to a PYQL_CLUSTER. This method ensure if the pyql-rest endpoint instance is restarted, that it will re-join automatically(important if network path changes). This is the method that k8s pyql-rest replicaStatefulSets use to ensure replica pods always update their path if a pod is terminated and restarted (due to scale down / scale up, rolling upgrade, manual deletion or liveness probe)
* Endpoints of type PYQL_TYPE=STANDALONE may join a cluster, but must manually invoke the joinCluster API to the pyql-cluster.

### Initializing a Data Cluster
A data cluster is created using the /cluster/{clusterName}/create API or by invoking /cluster/{clusterName}/join, with /cluster/{clusterName}/join, if the cluster does not yet exist for the user, the cluster is created. See other consideratoins noted above. 

A new Data-Cluster (without endpoints / tables) can be used after atleast 1 endpoint is added / created. Currently pyql-cluster does not orchestrate the deployment of new pyql-rest endpoints, but provides templates & generator scripts for getting started. 

#### Deploy data-cluster on K8S or Docker
Data-Cluster endpoints may be deployed as standalone containers or within a kubernetes statefulSet deployment. The power of kubernetes ability to scale in / out, schedule / reschedule failed instances makes this an attractive choice, and is what pyql-cluster & pyql-rest endpoints are designed primarily to utilize(but not required).

#### Data-Cluster - K8S
When a Data-Cluster is first created, typically this is created by a pyql-rest endpoint which links with an existing database. This is known as a 'seed' deployment, as this may be the original source of some tables in the cluster. These instances are typically deployed with a pre-existing database instance, but can also be deployed as a side-car with a new database instance.    

Pre-Requistites:
1. Registered User in pyql-cluster
2. JoinToken generated by registered user

[pyql-rest/k8s](https://github.com/codemation/pyql-rest/blob/master/k8s/) provides a generator script for creating & applying the appropiate configuration files within the current k8s namespace needed for both creating a seeding & replica deployment. This is demonstated beow:

Pull configuration Generator

        $ wget https://github.com/codemation/pyql-rest/blob/master/k8s/generate-pyql-rest.py

Generator Syntax:
     
        # Order of arguments is not important
        python generate-pyql-rest.py \ 
            --clusterid <name> # A unique name that should be associated with all deployments (replica & seed) for the same Data-Cluster, this name does not affect application, but affects k8s config, secret, & statefulSet file & object naming. 
            --tables <ALL|table1,table2 ..> # What tables are included from database added with endpoint
            --databases <database> # database rest-endpoint will connect to and joined with Data-Cluster 
            --dbtype <mysql|sqlite> 
            --dbuser <user> 
            --dbpassword <pw> 
            --dbport <port> # Port dbhost is listening on for db connections
            --dbhost 83.127.235.150 # host addres pyql-cluster & pyql-rest instances in K8s will use to access 
            --token <token> # Generated join token
            --clustername <cluster> # name of Data-Cluster to init/join
            --tag <tag>  # unique special identifier to describe database (deparment, site, location, etc ..). There can be many deployments with the same clusterid, but should only be 1 deployement with this tag
            --port <pyql_port> # Port which pyql-rest endpoint in k8s will listen for requests
            --pyqlclustersvc <pyql-cluster service|host:port # example pyql-cluster-lb.default.svc.cluster.local # if deployed in same k8s cluster as pyql-cluster, service name of pyql-cluster should be used, otherwise use pyql-cluster-hostname:port

Generator Example:

        # assumes an activated python3 env

        $ python generate-pyql-rest.py --clusterid cluster-data1 --tables ALL --databases joshdb --dbtype mysql --dbuser josh --dbpassword abcd1234 --dbport 3306 --dbhost 83.127.235.150 --token eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6ImViMzJkNDAyLWI2NjgtMTFlYS04ZThjLWVlZGQ2OTIyMGQ4NCIsImV4cGlyYXRpb24iOiJqb2luIiwiY3JlYXRlVGltZSI6MTU5MzAzNzI1OC4yMjY4OTkxfQ.g3sBiIXkEzfurwdGxqPV95hq90I6CO_b_Bhd0TnymAE --clustername data1 --tag sitea --port 80 --pyqlclustersvc pyql-cluster-lb.default.svc.cluster.local
        configmap/pyql-rest-cluster-config-cluster-data1 created
        configmap/pyql-rest-seed-db-config-cluster-data1-sitea created
        persistentvolume/pyql-rest-seed-pv-cluster-data1-sitea created
        secret/pyql-rest-seed-db-secrets-cluster-data1-sitea created
        secret/pyql-rest-replica-mysql-secrets-cluster-data1 created
        configmap/pyql-rest-replica-mysql-config-cluster-data1 created
        configmap/pyql-rest-replica-config-cluster-data1 created
        secret/pyql-rest-replica-secrets-cluster-data1 created

        # configuratoin files created
        $ ls *.yaml
        pyql-rest-cluster-config-cluster-data1-sitea.yaml        pyql-rest-replica-config-cluster-data1.yaml        pyql-rest-seed-db-config-cluster-data1-sitea.yaml
        pyql-rest-cluster-seed-ss-init-cluster-data1-sitea.yaml  pyql-rest-replica-mysql-config-cluster-data1.yaml  pyql-rest-seed-pv-cluster-data1-sitea.yaml
        pyql-rest-cluster-seed-ss-join-cluster-data1-sitea.yaml  pyql-rest-replica-ss-cluster-data1.yaml

Generator applys configmaps & secrets right away and writes configmaps to files. Secrets are not written to files to avoid accidental commit into source code. 

To update the existing configmaps, secrets, or statefulSet, either re-run the generator( will overwrite existing files in path), or edit the .yaml config files. 


Deploy seed 

        #current pods
        $ kubectl get pods
        NAME             READY   STATUS    RESTARTS   AGE
        pyql-cluster-0   1/1     Running   0          2m4s
        pyql-cluster-1   1/1     Running   0          7m4s
        pyql-cluster-2   1/1     Running   0          4m21s

        # Deploy seed statefulSet
        $ kubectl apply -f pyql-rest-cluster-seed-ss-init-cluster-data1-sitea.yaml
        statefulset.apps/pyql-rest-seed-ss-cluster-data1-sitea configured

        # check seed status
        $ kubectl get pods --selector pyql-rest-cluster-name=data1
        pyql-rest-seed-ss-cluster-data1-sitea-0   1/1     Running   0          7s

With the seed deployed, the seed with be accessible via the pyql-cluster URI: http://pyql-cluster/{cluster name}/data1/tables/{table name} APIs for querying & updating. The data querying APIs are very closely match [pyql-rest](https://github.com/codemation/pyql-rest) in that data is accessed via /cluster/{cluster name}/table/{table name} instead of /db/{db name}/table/{table name}. 

Deploy replicas

        # deploy replicas for cluster
        $ kubectl apply -f pyql-rest-replica-ss-cluster-data1.yaml

        # check pods in cluster
        $ kubectl get pods --selector pyql-rest-cluster-name=data1
        pyql-rest-replica-ss-cluster-data1-0      0/2     Pending   0          68s
        pyql-rest-seed-ss-cluster-data1-sitea-0   1/1     Running   0          2m20s

If the pod is 'described', the pending status is due to no available persistent volumes, as these must be manually provisioned with a storage class 'pyql-rest-pv-manual-sc' (default, but editable before deployment). The following helper script is also available for provisioning PV's. 

        # pull pv provisioner helper script
        $ wget https://github.com/codemation/pyql-rest/blob/master/k8s/generate_pvs.py

        # usage
        $ python generate_pvs.py <desired count>
        
        # example
        $ python generate_pvs.py 4
        persistentvolume/pyql-rest-pv-00 created
        persistentvolume/pyql-rest-pv-01 created
        persistentvolume/pyql-rest-pv-02 created
        persistentvolume/pyql-rest-pv-03 created

        # to add more, simply increase number
        $ python generate_pvs.py 6
        persistentvolume/pyql-rest-pv-00 unchanged
        persistentvolume/pyql-rest-pv-01 unchanged
        persistentvolume/pyql-rest-pv-02 unchanged
        persistentvolume/pyql-rest-pv-03 unchanged
        persistentvolume/pyql-rest-pv-04 created
        persistentvolume/pyql-rest-pv-05 created
    
Replica Pods should already begin creation matching statefulSet replicas count

        $ kubectl get pods --selector pyql-rest-cluster-name=data1
        NAME                                      READY   STATUS              RESTARTS   AGE
        pyql-rest-replica-ss-cluster-data1-0      0/2     ContainerCreating   0          72s
        pyql-rest-seed-ss-cluster-data1-sitea-0   1/1     Running             0          2m24s

        $ kubectl get pods --selector pyql-rest-cluster-name=data1
        NAME                                      READY   STATUS    RESTARTS   AGE
        pyql-rest-replica-ss-cluster-data1-0      2/2     Running   0          2m4s # replica 1
        pyql-rest-replica-ss-cluster-data1-1      2/2     Running   0          49s  # replica 2 
        pyql-rest-seed-ss-cluster-data1-sitea-0   1/1     Running   0          3m16s

Note the 2/2 in READY column for the replicas. This is because the pod is supporting 2 containers,  pyql-rest & mysql as a sidecar container. 

The final step in this deployment is to convert the seed deployment from 'init' to 'join'. This step ensures that the seed endpoint will re-connect to the cluster, when pod is re-scheduled,  if deleted, terminated. 

        # convert seed from init -> join
        $ kubectl apply -f pyql-rest-cluster-seed-ss-join-cluster-data1-sitea.yaml
        statefulset.apps/pyql-rest-seed-ss-cluster-data1-sitea configured

        # check pods in cluster
        $ kubectl get pods --selector pyql-rest-cluster-name=data1
        NAME                                      READY   STATUS        RESTARTS   AGE
        pyql-rest-replica-ss-cluster-data1-0      2/2     Running       0          25h
        pyql-rest-replica-ss-cluster-data1-1      2/2     Running       0          25h
        pyql-rest-seed-ss-cluster-data1-sitea-0   0/1     Terminating   0          25h

        $ kubectl get pods --selector pyql-rest-cluster-name=data1
        NAME                                      READY   STATUS    RESTARTS   AGE
        pyql-cluster-0                            1/1     Running   0          2d
        pyql-cluster-1                            1/1     Running   0          2d
        pyql-cluster-2                            1/1     Running   0          2d
        pyql-rest-replica-ss-cluster-data1-0      2/2     Running   0          25h
        pyql-rest-replica-ss-cluster-data1-1      2/2     Running   0          25h
        pyql-rest-seed-ss-cluster-data1-sitea-0   1/1     Running   0          5s

During termination, access to the data was still acccessible via the replicas, likewise changes could be made and the seed instance will be synced upon re-join to cluster.

#### Docker

This is a single running instance, which must be managed manually restarted via other tools

Requirements:
- New or existing Database
- Volume path specified on container creation, path location must be managed & re-used manually. 

See [pyql-rest](https://github.com/codemation/pyql-rest) example


## Lifecycle of a pyql-cluster
A pyql-cluster shares similar traits with that of a data-cluster:
- Comprised of a self-healing, scalable table cluster
- Masterless & load-balanced access to cluster tables
- Generally contrained to a single k8s cluster to benifit from the native service load-balancing.

### Deploy
A pyql-cluster can be deployed as a single node and scaled to many nodes. Once deployed the pyql-cluster can be scaled out for re-dundancy immedietly or at a later time. 

#### 1. Download, edit, and apply init_config 

    PYQL_CLUSTER_SVC: Service Path used by pyql-cluster endpoints (k8s pods) to join pyql-cluster. The service name should match with the name defined in[pyql-cluster-statefulSetInit.yaml](./init/pyql-cluster-statefulSetInit.yaml)
     
        Syntax  -  pyql-cluster-lb.<k8s-namespace>.svc.<k8scluster.domain>
        Example - 'pyql-cluster-lb.default.svc.cluster.local'

    PYQL_PORT - TCP Port used to access PYQL_CLUSTER_SVC

        kubectl apply -f 0-pyql-cluster-config.yaml
        
        From Source Code:default name-space & cluster.local
        kubectl apply -f https://github.com/codemation/pyql-cluster/blob/master/k8s/init_conig/0-pyql-cluster-config.yaml

#### 2. Deploy Persistent volumes which will store pyql-cluster data

    Basic:

        hostPath persistent volume
        -- 
        apiVersion: v1
        kind: PersistentVolume
        metadata:
        name: pyql-pv-volume-00
        labels:
            type: local
        spec:
        storageClassName: pyql-pv-manual-sc
        capacity:
            storage: 20Gi
        accessModes:
            - ReadWriteOnce
        hostPath:
            path: "/mnt/pyql-pv-volume-00"
        
        OR 

        #Deploys 3 type hostPath pyql-cluster volumes with storageClassName: pyql-pv-manual-sc
        kubectl apply -f https://github.com/codemation/pyql-cluster/blob/master/k8s/init_conig/1-pyql-cluster-pv.yaml

        OR

        $ wget https://github.com/codemation/pyql-cluster/blob/master/k8s/init_config/generate_pvs.py
        $ python3 generate_pvs.py 1
        persistentvolume/pyql-pv-volume-00 configured

#### 3. Create Init Admin secret

        $ wget https://github.com/codemation/pyql-cluster/blob/master/k8s/init_conig/create_admin_secret.sh

        $ ./create_admin_secret.sh aVerySecretAdminPassword
        creating secret with encoded password YVZlcnlTZWNyZXRBZG1pblBhc3N3b3Jk
        secret/pyql-cluster-init-auth configured

#### 4. Deploy Cluster 

        #Pull edit & deploy 
        $ wget https://github.com/codemation/pyql-cluster/blob/master/k8s/init/pyql-cluster-statefulSetInit.yaml
        $ kubectl apply -f pyql-cluster-statefulSetInit.yaml
        statefulset.apps/pyql-cluster created
        service/pyql-cluster-lb created


        Or

        # deploy from source
        $ kubectl apply -f https://github.com/codemation/pyql-cluster/blob/master/k8s/init/pyql-cluster-statefulSetInit.yaml
        statefulset.apps/pyql-cluster created
        service/pyql-cluster-lb created
        
Note: In this example 'service/pyql-cluster-lb' is created as a type NodePort service which makes the pyql-cluster accessible on the NodePort of each k8s node, but can be modified to use a [LoadBalancer](https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/)

        $ kubectl get service pyql-cluster-lb
        NAME              TYPE       CLUSTER-IP      EXTERNAL-IP   PORT(S)        AGE
        pyql-cluster-lb   NodePort   10.97.143.170   <none>        80:32543/TCP   4d3h

#### 5. Expand Pyql-Cluster (Optional): 
Prevent single pod of failure by adding pyql-cluster endpoints


Using Admin Password - Pull Join Token from http://pyql-cluster-lb/auth/token/join 

        $ curl -H 'Content-Type: application/json' -H "Authentication: Basic $(echo 'admin:aVerySecretAdminPassword' | base64)" http://pyql-cluster:32543/auth/token/join
        {"join":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjU4Yjk0MTc0LWIyZWUtMTFlYS05N2MzLTk2MDY1YTNhMWE1NSIsImV4cGlyYXRpb24iOiJqb2luIiwiY3JlYXRlVGltZSI6MTU5MjY1NTU4MC4xNzMxOTUxfQ.Z-KytDTrS_1Q4hyScEjgxA7eT5lCJHV3ukhuI2_iYbM"}


Using Join Token - Create Join Secret

        # Pull token scret creation script
        $ https://github.com/codemation/pyql-cluster/blob/master/k8s/join_config/create_secret_join_token.sh

        # Generate pyql-cluster-join-token secret
        $ ./create_secret_join_token.sh eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjU4Yjk0MTc0LWIyZWUtMTFlYS05N2MzLTk2MDY1YTNhMWE1NSIsImV4cGlyYXRpb24iOiJqb2luIiwiY3JlYXRlVGltZSI6MTU5MjY1NTU4MC4xNzMxOTUxfQ.Z-KytDTrS_1Q4hyScEjgxA7eT5lCJHV3ukhuI2_iYbM
        creating secret with token eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjU4Yjk0MTc0LWIyZWUtMTFlYS05N2MzLTk2MDY1YTNhMWE1NSIsImV4cGlyYXRpb24iOiJqb2luIiwiY3JlYXRlVGltZSI6MTU5MjY1NTU4MC4xNzMxOTUxfQ.Z-KytDTrS_1Q4hyScEjgxA7eT5lCJHV3ukhuI2_iYbM
        secret/pyql-cluster-join-token configured

Add new PVs to support new PV endpoints

        $ python3 generate_pvs.py 3
        persistentvolume/pyql-pv-volume-00 unchanged
        persistentvolume/pyql-pv-volume-01 configured
        persistentvolume/pyql-pv-volume-02 configured 

Begin expansion
        
        # Pull edit & deploy 
        $ wget https://github.com/codemation/pyql-cluster/blob/master/k8s/pyql-cluster-statefulSet.yaml

        # edit desired number of replicas 
        $ kubectl apply -f pyql-cluster-statefulSet.yaml
        statefulset.apps/pyql-cluster configured

        OR 

        # From Source - Expands Cluster to 3 endpoints - using defaults
        $ kubectl apply -f https://github.com/codemation/pyql-cluster/blob/master/k8s/pyql-cluster-statefulSet.yaml

        $ kubectl apply -f pyql-cluster-statefulSet.yaml 
        statefulset.apps/pyql-cluster configured

Monitor Expansion - each node make take up to 3 minutes to be 'ready' which allows endpoint to service pyql-cluster requests. 

        $ kubectl get pods
        NAME             READY   STATUS    RESTARTS   AGE
        pyql-cluster-0   1/1     Running   0          31m
        pyql-cluster-1   0/1     Running   0          103s

        $ kubectl get pods
        NAME             READY   STATUS    RESTARTS   AGE
        pyql-cluster-0   1/1     Running   0          2m
        pyql-cluster-1   1/1     Running   0          5m38s
        pyql-cluster-2   1/1     Running   0          3m51s

### Upgrades
Upgrades on pyql-clusters of size 3 or greater, can be performed with zero downtime with the help of rolling upgrades in k8s & pyql-cluster quorum mechaics. At least 2 endpoints out of every 3 must be online for 'quorum' which permits service requests. 

Steps To upgrade / downgrade the version of pyql-cluster:  
1. Select desired image from [DockerHub](https://hub.docker.com/r/joshjamison/pyql-cluster/tags)
2. Edit statefulset statefulSet with new image:tag
3. Apply change

        # PYQL UPGRADE - to selected version in pyql-cluster-statefulSet.yaml
        $ kubectl apply -f pyql-cluster-statefulSet.yaml

        # PYQL UPGRADE from source to latest
        $ kubectl apply -f https://github.com/codemation/pyql-cluster/blob/master/k8s/pyql-cluster-statefulSet.yaml

        # Rolling Upgrade Begin 
        $ kubectl get pods
        NAME             READY   STATUS        RESTARTS   AGE
        pyql-cluster-0   1/1     Running       0          51m
        pyql-cluster-1   1/1     Running       0          55m
        pyql-cluster-2   1/1     Terminating   0          53m

        # After node is online with new version & ready, next node will begin
        $ kubectl get pods
        NAME             READY   STATUS        RESTARTS   AGE
        pyql-cluster-0   1/1     Running       0          53m
        pyql-cluster-1   1/1     Terminating   0          56m
        pyql-cluster-2   1/1     Running       0          72s

        # All nodes report ready once completed
        $ kubectl get pods
        NAME             READY   STATUS    RESTARTS   AGE
        pyql-cluster-0   1/1     Running   0          2m20s
        pyql-cluster-1   1/1     Running   0          5m8s
        pyql-cluster-2   1/1     Running   0          9m40s

### Node Outage
pyql-cluster can continue to service data-cluster & pyql-cluster requests as long as the pyql-cluster maintains a 2/3 quorum with cluster members. A cluster of 3 can support a single endpoint outage(termination / network connectivity, other). A cluster of 6 can support 2 endpoint failures and so on ... 

## PYQL-CLUSTER API REFERENCE
Request Content-Type & Response Content-Type will always default to application/json

| ACTION | HTTP Verb | Path             | Auth Required | Request body | Example response body |
|--------|-----------|------------------|---------------|--------------|-----------------------|
| Register a new user in pyql-cluster | POST | /auth/{user or admin}/register | pyql | {"username":"aNewUser","email":"newUser@aCompany.com","password":"abcd1234"} | {"message":"user created successfully"} |
| Create a user authenticatoin token, to be used in place of user/pw in header auth, expires after 3600 seconds | GET | /auth/token/user | cluster | none | {"token": "token...." |
| Create a user join token, can only be used to join a cluster owned by user | GET | /auth/token/join | cluster | none | {"join": "token..."} |
| Get all rows from cluster table | GET | /cluster/{cluster}/table/{table}' | cluster | none | {"data":[ <br>{"department_id":1001,"id":100101,"name":"Director"}, <br>{"department_id":1001,"id":100102,"name":"Manager"}, <br>{"department_id":1001,"id":100103,"name":"Rep"}, <br>{"department_id":1001,"id":100104,"name":"Intern"},  <br>.. ]} |
| Create row in cluster table | POST, PUT | /cluster/{cluster}/table/{table}' | cluster | {"afterHours":true,"date":"2006-01-08","price":33.16,"qty":null,"symbol":null,"trans":{"condition":{"limit":"34.00","time":"EndOfTradingDay"},"type":"BUY"}}  | {<br> "consistency":true, <br> "message":{<br> "153a4f96-b9d9-11ea-93ec-feed8b92afdf":{"content":{"message":{"message":"items added","status":200},"status":200}<br>.... <br>} |
| Get all rows from cluster table | GET | /cluster/{cluster}/table/{table}/select' | cluster | none | {"data":[<br> {"id":1000,"name":"Clara Franklin","position_id":100101},<br>{"id":1001,"name":"Eli Carson","position_id":100102},<br>{"id":1002,"name":"Joe Smith","position_id":100102}, <br> ..]} |
| Get Specfied Rows from cluster table <br><br>Optional: matching specific column values <br><br>Optional: Join Specified tables  | POST | /cluster/{cluster}/table/{table}/select' | cluster |  ## Example 1<br> {"select":["*"],"where":{"column1":"value1"}} <br><br> ## Example2 <br>{"select":["*"],"join": "positions"} <br> Other examples at [pyql-rest](https://github.com/codemation/pyql-rest)  | ## Example 1 <br>  {"data":[<br>{"id":1000,"name":"Clara Franklin","position_id":100101}, <br>{"id":1001,"name":"Eli Carson","position_id":100102},<br>{"id":1002,"name":"Joe Smith","position_id":100102}, <br> ..]} <br><br> ## Example 2 <br>{"data":[<br>{"employees.id":1000,"employees.name":"Clara Franklin","employees.position_id":100101,"positions.department_id":1001,"positions.name":"Director"}, <br>{"employees.id":1001,"employees.name":"Eli Carson","employees.position_id":100102,"positions.department_id":1001,"positions.name":"Manager"},<br> .. ]} |
| Get Row with Primary Key Value | GET | /cluster/{cluster}/table/{table}/{key} | cluster | none | {"data":[{"id":1000,"name":"Clara Franklin","position_id":100101}]} |
| Update Row with Primary Key Value | POST | /cluster/{cluster}/table/{table}/{key}  | cluster | {"name": "Clara Franklin-Rogers"} | {<br>"consistency":true,<br>"message":{<br>"153a4f96-b9d9-11ea-93ec-feed8b92afdf":{"content":{"message":{"message":"OK","status":200},"status":200},"status":200}, .. <br>... <br>} |
| Delete Row with Primary Key Value | DELETE | /cluster/{cluster}/table/{table}/{key}  | cluster | none | {"consistency":true,"message":{"153a4f96-b9d9-11ea-93ec-feed8b92afdf":{"content":{"message":{"message":"OK","status":200},"status":200},"status":200},<br>... <br>}|
| Create row in cluster table | POST | /cluster/{cluster}/table/{table}/insert | cluster | {"afterHours":true,"date":"2006-01-09","price":32.16,"qty":null,"symbol":null,"trans":{"condition":{"limit":"34.00","time":"EndOfTradingDay"},"type":"BUY"}}  | {<br> "consistency":true, <br> "message":{<br> "153a4f96-b9d9-11ea-93ec-feed8b92afdf":{"content":{"message":{"message":"items added","status":200},"status":200}<br>.... <br>} |
| Update row in cluster table matching condition | POST | /cluster/{cluster}/table/{table}/update | cluster | {"set":{"position_id":200102},"where":{"id":1001}} | {<br>"consistency":true,<br>"message":{<br>"153a4f96-b9d9-11ea-93ec-feed8b92afdf":{"content":{"message":{"message":"OK","status":200},"status":200},"status":200}, .. <br>... <br>}}  |
| Delete row in cluster table  matching condition | POST | /cluster/{cluster}/table/{table}/delete | cluster | {"where":{"id": 1003}} | {"consistency":true,"message":{"153a4f96-b9d9-11ea-93ec-feed8b92afdf":{"content":{"message":{"message":"OK","status":200},"status":200},"status":200},<br>... <br>} |
| Get cluster table config | GET | /cluster/{cluster}/table/{table}/config | cluster | none | {"employees":{"columns":[{"mods":"NOT NULL","name":"id","type":"int"},{"mods":"","name":"name","type":"str"},{"mods":"DEFAULT NULL","name":"position_id","type":"int"}],"foreign_keys":{"position_id":{"mods":" ON DELETE CASCADE ON UPDATE CASCADE","ref":"id","table":"positions"}},"primary_key":"id"}}|
| Get all cluster tables config | GET | /cluster/{cluster}/tables | cluster | none | {"departments":{"columns":[{"mods":"NOT NULL","name":"id","type":"int"},{"mods":"","name":"name","type":"str"}],"foreign_keys":null,"primary_key":"id"},"employees":{"columns":[{"mods":"NOT NULL","name":"id","type":"int"},{"mods":"","name":"name","type":"str"},{"mods":"DEFAULT NULL","name":"position_id","type":"int"}],"foreign_keys":{"position_id":{"mods":" ON DELETE CASCADE ON UPDATE CASCADE","ref":"id","table":"positions"}},"primary_key":"id"}, <br>...<br>} |
| Join Data Cluster | POST | /cluster/{cluster name}>/join | cluster join_token  | {<br> "name": "192-168-231-226",<br>  "path": "192.168.231.226:80",<br>    "token":"token.....",<br> "database": {"name": "company", "uuid": "bf4a5a9a-ba00-11ea-97d8-5647a99b32bb"},<br> "tables": [],<br> "consistency": ["employees", "positions", "departments"]<br>} | {"message": "join cluster {cluster name} for endpoint 192-168-231-226 completed successfully")} |

For advanced pyql json queries, join, multi-join syntax, and more pyql json examples, see [pyql-rest](https://github.com/codemation/pyql-rest). Syntax for pyql json queries does not differ between pyql-cluster and [pyql-rest](https://github.com/codemation/pyql-rest), only the API URI /cluster/{cluster}/table/{table} versus /db/{database}/table/{table} 

## PYQL-CLUSTER INTERNAL API REFERENCE
Request Content-Type & Response Content-Type will always default to application/json

| ACTION | HTTP Verb | Path             | Auth Required | Request body | Example response body |
|--------|-----------|------------------|---------------|--------------|-----------------------|
| Updates a local pyql endpoint with a new local encryption/decryption key, used by key rotation or initial config | POST | /auth/key/{cluster or local} | local | {'PYQL_LOCAL_TOKEN_KEY': 'key....'} or {'PYQL_CLUSTER_TOKEN_KEY': 'key....'} | {"message": "{location} updated successfully with {key}")} |
|  Used primary to update joining nodes with a PYQL_CLUSTER_SERVICE_TOKEN so joining node can pull and set its PYQL_CLUSTER_TOKEN_KEY | POST | /auth/setup/cluster | local | {"PYQL_CLUSTER_SERVICE_TOKEN": "token...."} | {"message": "{location} updated successfully with {key}")} |
| Get current PYQL_CLUSTER_SERVICE_TOKEN or PYQL_CLUSTER_SERVICE_TOKEN | GET | /auth/token/{cluster or local} | pyql | none | {"PYQL_{local or cluster}_SERVICE_TOKEN": "token....} |
| Get current PYQL_CLUSTER_TOKEN_KEY or PYQL_CLUSTER_TOKEN_KEY | GET | /auth/key/{cluster or local} | pyql | none | {"PYQL_{local or cluster}_TOKEN_KEY": "key....} |
| GET endpoint UUID | GET | /pyql/node | none | none | {"uuid": "node_uuid..." |
| Check the readieness of a pyql endpoint by re-triggering a quorum check & verifying local state table is in_sync | GET | /cluster/pyql/ready | none | none | {"health":"healthy","inQuorum":true,"lastUpdateTime":1593385795.2684991,"missing":{"nodes":[]},"node":"c49d32b4-b66e-11ea-b31d-eef4a54d64d8","nodes":{"nodes":["276eeb80-b66f-11ea-81a6-d678bdb9f7b8","779d5b3c-b66f-11ea-aebe-4ecfe07dbf34","c49d32b4-b66e-11ea-b31d-eef4a54d64d8"]},"ready":true} |
| Triggers a quorum check on each node in pyql-cluster | POST | /pyql/quorum/check | pyql | none | {"message":"cluster_quorum_check completed on c49d32b4-b66e-11ea-b31d-eef4a54d64d8","results":{"276eeb80-b66f-11ea-81a6-d678bdb9f7b8":{"content":{"message":"cluster_quorum_update on node 276eeb80-b66f-11ea-81a6-d678bdb9f7b8 updated successfully","quorum":{"health":"healthy","inQuorum":true,"lastUpdateTime":1593386168.311425,"missing":{"nodes":[]},"node":"276eeb80-b66f-11ea-81a6-d678bdb9f7b8","nodes":{"nodes":["c49d32b4-b66e-11ea-b31d-eef4a54d64d8","779d5b3c-b66f-11ea-aebe-4ecfe07dbf34","276eeb80-b66f-11ea-81a6-d678bdb9f7b8"]},"ready":true}},"status":200}, ...} |
| Get current quorum status of endpoint | GET | /pyql/quorum | local | none | {"health":"healthy","inQuorum":true,"lastUpdateTime":1593386168.311425,"missing":{"nodes":[]},"node":"276eeb80-b66f-11ea-81a6-d678bdb9f7b8","nodes":{"nodes":["c49d32b4-b66e-11ea-b31d-eef4a54d64d8","779d5b3c-b66f-11ea-aebe-4ecfe07dbf34","276eeb80-b66f-11ea-81a6-d678bdb9f7b8"]} |
| Update quorum status of endpoint | POST  | /pyql/quorum | local | none | {"health":"healthy","inQuorum":true,"lastUpdateTime":1593386168.311425,"missing":{"nodes":[]},"node":"276eeb80-b66f-11ea-81a6-d678bdb9f7b8","nodes":{"nodes":["c49d32b4-b66e-11ea-b31d-eef4a54d64d8","779d5b3c-b66f-11ea-aebe-4ecfe07dbf34","276eeb80-b66f-11ea-81a6-d678bdb9f7b8"]} |
| Get endpoint paths to cluster tables | GET | /cluster/{cluster_uuid}/table/{table}/path | pyql | body | {"in_sync":{"276eeb80-b66f-11ea-81a6-d678bdb9f7b8":"http://192.168.200.225:80/db/cluster/table/state","779d5b3c-b66f-11ea-aebe-4ecfe07dbf34":"http://192.168.231.201:80/db/cluster/table/state","c49d32b4-b66e-11ea-b31d-eef4a54d64d8":"http://192.168.231.238:80/db/cluster/table/state"},"out_of_sync":{}}|
| Get information on all cluster table endpoints | GET | /cluster/{cluster_uuid}/table/{table}/endpoints | pyql | none | {"clusterName":"pyql","in_sync":{"276eeb80-b66f-11ea-81a6-d678bdb9f7b8":{"cluster":"8eac4730-b66e-11ea-ac85-6ae89b450a37","dbname":"cluster","in_sync":true,"lastModTime":0.0,"name":"276eeb80-b66f-11ea-81a6-d678bdb9f7b8state","path":"192.168.200.225:80","state":"loaded","table_name":"state","token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjI3YTAxNDU4LWI2NmYtMTFlYS04MWE2LWQ2NzhiZGI5ZjdiOCIsImV4cGlyYXRpb24iOiJuZXZlciJ9.fCRVSSLiAe6bWE3s9reKd6csGBsEr5h_NZ6NspYEEGQ","uuid":"276eeb80-b66f-11ea-81a6-d678bdb9f7b8"}, .. }},"out_of_sync":{}} |
|  Get information on specific table endpoint | GET | /cluster/{cluster_uuid}/table/{table}/{} | local_pyql_cluster | body | body_response |