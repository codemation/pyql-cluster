 # database
def attach_tables(server):
    #Tables are added  here
    pass
    from dbs.database.tables import nodes
    nodes.db_attach(server)
            
    from dbs.database.tables import cluster
    cluster.db_attach(server)
            
    from dbs.database.tables import tables
    tables.db_attach(server)
            
    from dbs.database.tables import databases
    databases.db_attach(server)
            