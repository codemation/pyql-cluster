
def run(server):
    pass # apps start here
    def check_db_table_exist(database,table):
        if not database in server.data:
            server.db_check(database)
        if database in server.data:
            if not table in server.data[database].tables:
                server.db_check(database)
            if table in server.data[database].tables:
                return "OK", 200
            else:
                return {'status': 404, 'message': f'table with name {table} not found in database {database}'}, 404   
        else:
            return {'status': 404, 'message': f'database with name {database} not found'}, 404
    server.check_db_table_exist = check_db_table_exist
            
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

    from apps.cache import cache
    cache.run(server)
      
    from apps.cluster import cluster
    cluster.run(server)            
