async def run(server):
    from fastapi import Request
    import os, uuid, time, json, asyncio
    log = server.log
    txn_default_wait_in_sec = 0.005     # default 5 ms
    txn_max_wait_interval_in_sec = 0.050 # 50 ms
    txn_max_wait_time_in_sec = 0.550     # 550 ms 
    # pull stats from docker instance
    # docker logs -f pyql-cluster-8090 2>&1 | grep '##cache commit' | grep 'WARNING' | awk '{print  $8" "$9" "$10" "$12}'

    @server.api_route('/db/{database}/cache/{table}/txn/{action}', methods=['POST'])
    async def cache_txn_manage_endpoint(database:str, table: str, action: str, transaction: dict, request: Request):
        return await cache_txn_manage(database, table, action, transaction,  request=await server.process_request(request))
    @server.is_authenticated('local')
    @server.trace
    async def cache_txn_manage(database, table, action, transaction, **kw):
        """
            method for managing txns - canceling / commiting
        """
        trace = kw['trace']
        cache = server.data[database].tables['cache']
        if 'txn' in transaction:
            txn_id = transaction['txn']
            tx=None
            wait_time = 0.0                      # total time waiting to commit txn 
            wait_interval = txn_default_wait_in_sec  # amount of time to wait between checks - if multiple txns exist 
            # Get transaction from cache db
            if action == 'commit':
                while True:
                    txns = await cache.select('id','timestamp',
                        where={'table_name': table}
                    )
                    if not txn_id in {tx['id'] for tx in txns}:
                        server.http_exception(500, trace.error(f"{txn_id} does not exist in cache"))
                    if len(txns) == 1:
                        if not txns[0]['id'] == txn_id:
                            warning = f"txn with id {txn_id} does not exist for {database} {table}"
                            server.http_exception(500, trace.warning(warning))
                        # txn_id is only value inside
                        tx = txns[0]
                        break
                    # multiple pending txns - need to check timestamp to verify if this txn can be commited yet
                    txns = sorted(txns, key=lambda txn: txn['timestamp'])
                    for ind, txn in enumerate(txns):
                        if txn['id'] == txn_id:
                            if ind == 0:
                                tx = txns[0]
                                break
                            if wait_time > txn_max_wait_time_in_sec:
                                warning = f"timeout of {wait_time} reached while waiting to commit {txn_id} for {database} {table}, waiting on {txns[:ind]}"
                                trace.warning(warning)
                                trace.warning(f"removing txn with id {txns[0]['id']} maxWaitTime of {txn_max_wait_time_in_sec} reached")
                                await cache.delete(where={'id': txns[0]['id']})
                                break
                            break
                    if tx == None:
                        trace.warning(f"txn_id {txn_id} is behind txns {txns[:ind]} - waiting {wait_time} to retry")
                        await asyncio.sleep(wait_interval)
                        wait_time+=wait_interval 
                        # wait_interval scales up to txn_max_wait_interval_in_sec
                        wait_interval+=wait_interval 
                        if wait_interval >= txn_max_wait_interval_in_sec:
                            wait_interval = txn_max_wait_interval_in_sec
                        continue
                    break
                # Should not have broken out of loop here without a tx
                if tx == None:
                    trace.error("tx is None, this should not hppen")
                    server.http_exception(500, "tx was none")
                tx = await cache.select('type','txn',
                        where={'id': txn_id})
                tx = tx[0]
                try:
                    response = await server.actions[tx['type']](database, table, tx['txn'])
                    trace.warning(f"##cache {action} response {response}")
                except Exception as e:
                    trace.exception(f"Exception when performing cache {action} - response {response}")
                
                del_txn = await cache.delete(
                    where={'id': txn_id}
                )
                if response:
                    # update last txn id
                    set_params = {
                        'set': {
                            'last_txn_uuid': txn_id,
                            'last_mod_time': float(time.time())
                            },
                        'where': {
                            'table_name': table
                        }
                    }
                    await server.data['cluster'].tables['pyql'].update(
                            **set_params['set'],
                            where=set_params['where']
                    
                    )
                    return response
                server.http_exception(500, "")
            if action == 'cancel':
                del_txn = await cache.delete(
                    where={'id': txn_id}
                )
                return {'deleted': txn_id}

    @server.api_route('/db/{database}/cache/{table}/{action}/{txuuid}', methods=['POST'])
    async def cache_action_endpoint(database: str, table: str, action: str, txuuid: str, txn: dict, request: Request):
        return await cache_action(database, table, action, txuuid, txn, request=await server.process_request(request))

    @server.is_authenticated('local')
    async def cache_action(database, table, action, txuuid, txn, **kw):
        await server.data[database].tables['cache'].insert(**{
            'id': txuuid,
            'table_name': table,
            'type': action,
            'timestamp': txn['time'],
            'txn': txn['txn']
        })
        return {"txn": txuuid}