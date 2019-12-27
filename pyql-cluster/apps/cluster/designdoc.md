# Problems to solve


## How to handle failures to update state table 
The state table has the following columns:

    ('name', str, 'UNIQUE NOT NULL'),
    ('state', str),
    ('inSync', bool),
    ('uuid', str), # used for syncing logs 
    ('lastModTime', float)

The running server also mantains an in-memory version of this table for use in determining available endpoints for tables.

Examples assume the most basic environment:

Cluster: 
    pyql - used internally
        endpoints:
            pyql-master-node01
            pyql-master-node02
        state:
            pyql-master-node01state
            pyql-master-node02state


    example
        endpoints:
            example-restnode-01
            example-restnode-02
        tables:
            foo
            bar

## Problem 1:

    Incoming update into 'example' cluster targeting table foo. 
    The service routes the request to example-restnode-01 for handling & distributing update.

    node01 finds that there are no outOfSync endpoints for table 'foo' and attempts to update each endpoint db table.

     example-restnode-01 - success
     example-restnode-02 - failure

    As example-restnode-02 failed, the state of this endpoint should be marked as inSync=False

    This requires that each pyql cluster endpoint update its' state table and set example-restnode-02foo inSync=False

        pyql-master-node01 - set example-restnode-02foo inSync=False - success
        pyql-master-node02 - set example-restnode-02foo inSync=False - failure

    example-restnode-02 - failed to update its state table
    
    Need to check RC for failure:
        408 Request Timeout
        500 Server error while processing 
    
    In a PYQL cluster of two nodes, if one node cannot reach the other there is no way to determine if all table endpoints are consistent

    Examples Situation:
        - pyql-master-node01 is able to reach example-restnode-01 but not pyql-master-node02 or pyql-master-node02
        - pyql-master-node02 is able to reach example-restnode-02 but not pyql-master-node01 or pyql-master-node02
    If the above situation is allowed, the two DB endpoints may begin to deviate as writes handlers are distributed
        among master nodes.

### Solution:

State updates should require a quorum of 2/3 of pyql cluster nodes

| Total Nodes | Quorum Requirement |
|     1       |          1         |
|     2       |          2         |
|     3       |          2         |
|     4       |          3         |
|     5       |          4         |
|     6       |          4         |


