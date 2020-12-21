# table
async def run(server):
    from fastapi import Request, Depends
    from pydantic import BaseModel
    from typing import List, Optional
    import asyncio
    import time
    import uuid
    log = server.log

    PYQL_TABLE_DB = 'cluster'

    server.flush_table_tasks = {}
    server.pending_txns = {}

    """
    @server.api_route('/db/{database}/cache/{timestamp}')
    async def get_db_cache_by_timestamp_api(database: str, timestamp: float, request: Request):
        return await get_db_cache_by_timestamp(database, timestamp, request=await server.process_request(request))
    async def get_db_cache_by_timestamp(database, timestamp, **kw):
        cache_enabled = server.data[database].cache_enabled
        cache = None
        if timestamp in server.data[database].cache.timestamp_to_cache:
            cache = server.data[database].cache.timestamp_to_cache[timestamp]
        return {
            f'{timestamp}': cache if not cache == None else 'no cache found'
            }

    @server.api_route('/db/{database}/cache')
    async def get_db_cache_api(database: str, request: Request):
        return await get_db_cache(
            database,  
            request=await server.process_request(request)
        )
    
    async def get_db_cache(database, **kw):
        cache_enabled = server.data[database].cache_enabled
        return {
            'cache_enabled': cache_enabled,
            'cache': server.data[database].cache.cache if cache_enabled else {}
            }
    """

    @server.api_route('/db/{database}/tables')
    async def get_all_tables_api(database, request: Request, token: dict = Depends(server.verify_token)):
        return await get_all_tables(database,  request=await server.process_request(request))
    @server.is_authenticated('local')
    async def get_all_tables(database, **kw):
        if not database in server.data:
            server.http_exception(400, f"no database with name {database} attached to endpoint")
        tables = server.data[database].tables
        tables_config = []
        for table in tables:
            tables_config.append(
                {
                    table: {
                        "primary_key": tables[table].prim_key,
                        "foreign_keys": tables[table].foreign_keys,
                        "columns": [{"name": col.name,"type": str(col.type.__name__), 
                        "mods": col.mods } for k,col in tables[table].columns.items() ]
                    }
                }
            ) 
        return {"tables": tables_config}

    class ForeignKeyConfig(BaseModel):
        table: str = '<ref_table_name>'
        ref: str = '<ref_table_column>'
        mods: str = 'ON UPDATE CASCADE'

    class ForeignKey(BaseModel):
        col_name: ForeignKeyConfig

    class Table(BaseModel):
        name: str
        columns: List[List] = [
            [
                'col_name', 
                "int|str|bool|float|bytes", 
                "NOT NULL|UNIQUE|AUTOINCREMENT"
            ],
        ]
        prim_key: str
        foreign_keys: Optional[ForeignKey]
        cache_enabled: bool = True

    @server.api_route('/db/{database}/table/create', methods=['POST'])
    async def db_create_table_func(
        database: str, 
        config: Table, 
        request: Request,
        token: str = Depends(server.verify_token)
    ):
        return await create_table_auth(database, dict(config), request=await server.process_request(request))

    @server.is_authenticated('local')
    async def create_table_auth(database, config, **kw):
        return await create_table(database, config, **kw)
    
    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    @server.trace
    async def create_table(database, config, **kw):
        trace = kw['trace']
        if database in server.data:
            trace(f"called for {database} with config: {config}")
            return await server.data[database].create_table(**config)
        server.http_exception(404, f"database {database} not found")

    @server.api_route('/db/{database}/table/{table}', methods=['GET', 'PUT', 'POST'])
    async def db_table_api(database: str, table: str, request: Request, params: dict = None, token: dict = Depends(server.verify_token)):
        return await db_table(database, table, params=params,  request=await server.process_request(request))
    @server.is_authenticated('local')
    async def db_table(database, table, **kw):
        request = kw['request']
        if request.method == 'GET':
            return await db_table_get(database, table, **kw)
        if request.method == 'PUT' or request.method == 'POST':
            return await db_table_put_post(database, table, **kw)

    async def db_table_get(database, table, **kw):
        return await server.actions['select'](database, table, **kw)
    async def db_table_put_post(database, table, **kw):
        return await server.actions['insert'](database, table, **kw)



    @server.is_authenticated('local')
    async def db_table_key(database, table, key, **kw):
        request = kw['request']
        return await table_key(database, table, key, method=request.method, **kw)
    
    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def table_key(database, table, key, method='GET', **kw):
        primary_key = server.data[database].tables[table].prim_key
        if method == 'GET':
            return await db_table_key_get(database, table, primary_key, key)
        if method == 'POST':
            return await db_table_key_post(database, table, primary_key, key, set_vals=kw['params'])
        if method == 'DELETE':
            return await db_table_key_delete(database, table, primary_key, key)
    server.actions['select_key'] = table_key

    async def db_table_key_get(database, table, key, val):
        return await server.actions['select'](database, table, {'select': ['*'], 'where': {key: val}})
    
    async def db_table_key_post(database, table, key, val, set_vals=None):
        if set_vals == None:
            server.http_exception(
                400, 
                log.exception("db_table_key_post missing json input for values to update")
            )
        return await server.actions['update'](database, table, {'set': set_vals, 'where': {key: val}})
    async def db_table_key_delete(database, table, key, val):
        return await server.actions['delete'](database, table, {'where': {key: val}})

    """
    @server.api_route('/db/{database}/table/{table}/cache')
    async def db_get_table_cache_api(database: str, table: str, request: Request):
        return await db_get_table_cache(
            database,
            table,
            request=await server.process_request(request)
        )
    
    async def db_get_table_cache(database, table, **kw):
        cache_enabled = server.data[database].tables[table].cache_enabled
        return {
            'cache_enabled': cache_enabled,
            'cache': server.data[database].tables[table].cache.cache if cache_enabled else {}
            }
    """

    @server.api_route('/db/{database}/table/{table}/copy')
    async def db_get_table_copy_api(database: str, table: str, request: Request, token: dict = Depends(server.verify_token)):
        return await db_get_table_copy_auth(database, table,  request=await server.process_request(request))

    @server.is_authenticated('local')
    async def db_get_table_copy_auth(database,table, **kw):
        return await get_table_copy(database, table, **kw)

    async def get_table_copy(database, table, **kw):
        last_txn_time = await server.data[PYQL_TABLE_DB].tables['pyql'].select(
            'last_txn_time',
            where={
                'table_name': table
            }
        )
        last_txn_time = last_txn_time[0]['last_txn_time'] if len(last_txn_time) > 0 else time.time()
        table_copy = await server.data[database].tables[table].select('*')
        return {
            'last_txn_time': last_txn_time, 
            'table_copy': table_copy
            }
    server.get_table_copy = get_table_copy

    @server.api_route('/db/{database}/table/{table}/config')
    async def db_get_table_config_api(database: str, table: str, request: Request, token: dict = Depends(server.verify_token)):
        return await db_get_table_config(database, table,  request=await server.process_request(request))

    @server.is_authenticated('local')
    async def db_get_table_config(database,table, **kw):
        return await get_table_config(database, table, **kw)

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def get_table_config(database, table, **kw):
        message, rc = await server.check_db_table_exist(database, table)
        if not rc == 200:
            server.http_exception(rc, message)
        table = server.data[database].tables[table]
        return await table.get_schema()
            
    server.get_table_config = get_table_config

    @server.api_route('/db/{database}/table/{table}/create', methods=['POST'])
    async def database_table_create_api(database, table, config: dict, request: Request, token: dict = Depends(server.verify_token)):
        return await database_table_create(database, table, config, request=await server.process_request(request))

    @server.is_authenticated('local')
    async def database_table_create(database, table, config, **kw):
        return await create_table(database, config)


    # Table Flush
    """
        data = {
        "tx_cluster_path": (
            f"http://{os.environ['PYQL_CLUSTER_SVC']}/cluster/{txn_cluster_id}/table/{tx_table}"
        )
    }
    """
    @server.api_route('/db/{database}/table/{table}/flush', methods=['POST'])
    async def database_table_flush(database: str, table: str,  flush_cfg: dict, request: Request, token: dict = Depends(server.verify_token)):
        return await table_flush_auth(database, table, flush_cfg, request=await server.process_request(request))

    @server.is_authenticated('local')
    async def table_flush_auth(database: str, table: str,  flush_cfg: dict, **kw):
        return await table_flush_trigger(database, table, flush_cfg, **kw)

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)    
    async def table_flush_trigger(database: str, table: str,  flush_cfg: dict, **kw):
        async def table_flush_task():
            return await table_flush(database, table, flush_cfg, **kw)
        
        # flush tasks can over-ride each other, since each flush operation pulls 
        # subsequently more each time
        if not f"{database}_{table}" in server.flush_table_tasks:
            server.flush_table_tasks[f"{database}_{table}"] = {'work': 0, 'task': table_flush_task}

        count = server.flush_table_tasks[f"{database}_{table}"]['work']
        server.flush_table_tasks[f"{database}_{table}"]['task'] = table_flush_task
        count +=1
        async def flush_job():
            if 'task' in server.flush_table_tasks[f"{database}_{table}"]:
                job = server.flush_table_tasks[f"{database}_{table}"].pop('task')
                result = await job()
            else:
                result = "no flush work to perform"
            if server.flush_table_tasks[f"{database}_{table}"]['work'] > 0:
                server.flush_table_tasks[f"{database}_{table}"]['work'] -=1
            return {"table_flush_task_result": result}

        if count < 31:
            await server.flush.put(
                flush_job # table_flush_task # awaited via flush workers
            )
            log.warning(f"added a flush op for {database} {table} - {count} / 30")
        else:
            log.warning(f"max flush ops queued for {database} {table} - {count}")
        #server.flush.append(table_flush_task)
        return {"message": f"table flush triggered"}
    server.table_flush_trigger = table_flush_trigger

    async def table_flush(database: str, table: str, flush_cfg: dict, **kw):
        # pull last txn time 
        last_txn_time = await server.data[PYQL_TABLE_DB].tables['pyql'].select(
            'last_txn_time',
            where={
                'table_name': table
            }
        )
        last_txn_time = last_txn_time[0]['last_txn_time']
                
        log.warning(f"table_flush - table {table} pulling using last_txn_time {last_txn_time}")
        # Using flush_path - pull txns newer than the latest txn

        endpoint = flush_cfg['tx_cluster_endpoint']
        tx_cluster_id = flush_cfg['tx_cluster_id']
        tx_table = flush_cfg['tx_table']
        new_txns = await server.rpc_endpoints[endpoint]['table_select'](tx_cluster_id, tx_table)

        if not 'data' in new_txns:
            return log.error(f"ERROR encountered in table_flush - {new_txns} - releasing lock")
        new_txns = new_txns['data']
        # update latest txn
        update_last_txn_time = None
        flush_results = []
        for txn in new_txns:
            """
            in case of duplicate flush ops
            prevent same txn from being run
            """
            txn_timestamp = txn['timestamp']

            coros_to_gather = []
            for action, data in txn['txn'].items():
                if not update_last_txn_time == None:
                    coros_to_gather.append(update_last_txn_time)
                coros_to_gather.append(
                    server.actions[action](database, table, data)
                )
                flush_results.append(
                    await asyncio.gather(*coros_to_gather, return_exceptions=True)
                )
                update_last_txn_time = server.data[PYQL_TABLE_DB].tables['pyql'].update(
                    last_txn_time=txn['timestamp'],
                    where={"table_name": table}
                )
            
        if not update_last_txn_time == None:
            await update_last_txn_time
            
        # release table lock is issued by completely existing for loop
        return {
            "message": log.warning(f"{database} {table} table_flush completed successfully"), 
            "flush_results": flush_results
            }

    @server.api_route('/db/{database}/table/{table}/sync', methods=['POST'])
    async def sync_table_func_api(database: str, table: str, data_to_sync: dict, request: Request, token: dict = Depends(server.verify_token)):
        return await sync_table_auth(database, table, data_to_sync, request=await server.process_request(request))

    @server.is_authenticated('local')
    async def sync_table_auth(database, table, data_to_sync, **kw):
        return await sync_table(database, table, data_to_sync)

    @server.rpc.origin(namespace=server.PYQL_NODE_ID)
    async def sync_table(database, table, data_to_sync, **kw):
        if not database in server.data:
            server.http_exception(500, log.error(f"{database} not found in endpoint"))
        if not table in server.data[database].tables:
            await server.db_check(database)
        if not table in server.data[database].tables:
            server.http_exception(400, log.error(f"{table} not found in database {database}"))
        table_config = await get_table_config(database, table)

        message = await create_table(database, table_config)
        log.warning(f"table /sync create_table_func response {message}")
        #for row in data_to_sync['data']:
        #    log.warning(f"table sync insert row - {row}")
        #    await server.data[database].tables[table].insert(**row)
        rows_to_insert = [
            server.data[database].tables[table].insert(**row)
            for row in data_to_sync['table_copy']
            ]
        insert_results = await asyncio.gather(*rows_to_insert, return_exceptions=True)
        await server.data[PYQL_TABLE_DB].tables['pyql'].update(
            last_txn_time=data_to_sync['last_txn_time'],
            where={"table_name": table}
        )
        return {
            "message": log.warning(f"{database} {table} sync successful"), 
            "insert_results": insert_results
            }

    @server.api_route('/db/{database}/table/{table}/{key}', methods=['GET', 'POST', 'DELETE'])
    async def db_table_key_api(database: str, table: str, key: str, request: Request, params: dict = None, token: dict = Depends(server.verify_token)):
        return await db_table_key(database, table, key, params=params,  request=await server.process_request(request))