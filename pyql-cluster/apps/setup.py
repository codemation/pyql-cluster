
def run(server):
    pass # apps start here

            
    from apps.select import select
    select.run(server)            
            
    from apps.update import update
    update.run(server)            
            
    from apps.delete import delete
    delete.run(server)            
            
    from apps.insert import insert
    insert.run(server)            
            
    from apps.table import table
    table.run(server)            
            
    from apps.internal import internal
    internal.run(server)            
            
    from apps.cluster import cluster
    cluster.run(server)            
            