 # database
async def attach_tables(server):
    #Tables are added  here
    import asyncio
    db_attach_coros = []

    from dbs.database.tables import clusters
    db_attach_coros.append(clusters.db_attach(server))

    from dbs.database.tables import endpoints
    db_attach_coros.append(endpoints.db_attach(server))
            
    from dbs.database.tables import tables
    db_attach_coros.append(tables.db_attach(server))
            
    from dbs.database.tables import state
    db_attach_coros.append(state.db_attach(server))
            
    from dbs.database.tables import transactions
    db_attach_coros.append(transactions.db_attach(server))
            
    from dbs.database.tables import jobs
    db_attach_coros.append(jobs.db_attach(server))

    from dbs.database.tables import cache
    db_attach_coros.append(cache.db_attach(server))

    from dbs.database.tables import quorum
    db_attach_coros.append(quorum.db_attach(server))

    from dbs.database.tables import internaljobs
    db_attach_coros.append(internaljobs.db_attach(server))
            
    from dbs.database.tables import authlocal
    db_attach_coros.append(authlocal.db_attach(server))
            
    from dbs.database.tables import auth
    db_attach_coros.append(auth.db_attach(server))

    from dbs.database.tables import env
    db_attach_coros.append(env.db_attach(server))

    # process bulk db_attach 
    await asyncio.gather(*db_attach_coros)

    from dbs.database.tables import pyql
    await pyql.db_attach(server)
            
    from dbs.database.tables import data_to_txn_cluster
    await data_to_txn_cluster.db_attach(server)
            