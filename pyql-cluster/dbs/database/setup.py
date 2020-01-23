 # database
def attach_tables(server):
    #Tables are added  here
    pass
            
    from dbs.database.tables import clusters
    clusters.db_attach(server)

    from dbs.database.tables import endpoints
    endpoints.db_attach(server)
            
    from dbs.database.tables import tables
    tables.db_attach(server)
            
    from dbs.database.tables import state
    state.db_attach(server)
            
    from dbs.database.tables import transactions
    transactions.db_attach(server)
            
    from dbs.database.tables import jobs
    jobs.db_attach(server)

    from dbs.database.tables import cache
    cache.db_attach(server)

    from dbs.database.tables import quorum
    quorum.db_attach(server)

    from dbs.database.tables import pyql
    pyql.db_attach(server)
    from dbs.database.tables import internaljobs
    internaljobs.db_attach(server)
            