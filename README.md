# pyql-cluster
Manager for multiple pyql-rest endpoints to allow for read-replicas &amp; db mirroring

# Life of a Cluster

1. A single pyql cluster endpoint is deployed pyql-cluster-node-01 - no pyql-rest endpoints yet.

2. pyql cluster endpoint attempts to /cluster/pyql/join  via "service-name" to join "pyql" cluster with cluster database & tables.

    http://pyql-cluster/cluster/pyql/join

    No clusters exist yet in local "cluster" database table "clusters", so pyql is created with endpoints, db's, tables.
    This pyql cluster-endpoint is now and endpoint for cluster "pyql". 

    Clusters:
        pyql
            Endpoints:
                pyql-cluster-node-01

            Databases:
                cluster 

            Tables:
                clusters
                endpoints
                databases
                tables

3. A new pyql cluster endpoint is deployed pyql-cluster-node-02 via auto scaling or manually adjusting the deployment, still no pyql-rest endpoints yet

4. pyql-cluster-node-02 attempts to /cluster/pyql/join  via "service-name" to join "pyql" cluster with cluster database & tables.
    http://pyql-cluster/cluster/pyql/join 

    There should be a delay in time between pyql-cluster-node-02 creation & "Ready" state which ensures this new nodes is not used by "service-name" until it has joined cluster.
    
    This scaled pyql cluster endpoint also has its own "cluster" db with matching tables "clusters", "endpoints", 

5. 

