def run(server):
    from flask import request
    import os, uuid, time, json
    log = server.log
    txnDefaultWaitInSec = 0.005     # default 5 ms
    txnMaxWaitIntervalInSec = 0.050 # 50 ms
    txnMaxWaitTimeInSec = 0.550     # 550 ms 
    # pull stats from docker instance
    # docker logs -f pyql-cluster-8090 2>&1 | grep '##cache commit' | grep 'WARNING' | awk '{print  $8" "$9" "$10" "$12}'

    @server.route('/db/<database>/cache/<table>/txn/<action>', methods=['POST'])
    @server.is_authenticated('local')
    @server.trace
    def cache_txn_manage(database, table, action, trans=None, **kw):
        """
            method for managing txns - canceling / commiting
        """
        trace = kw['trace']
        cache = server.data[database].tables['cache']
        transaction = request.get_json() if trans == None else trans
        if 'txn' in transaction:
            txnId = transaction['txn']
            txnOrder = None
            tx=None
            waitTime = 0.0                      # total time waiting to commit txn 
            waitInterval = txnDefaultWaitInSec  # amount of time to wait between checks - if multiple txns exist 
            # Get transaction from cache db
            if action == 'commit':
                while True:
                    txns = cache.select('id','timestamp',
                        where={'tableName': table}
                    )
                    if not txnId in {tx['id'] for tx in txns}:
                        return {"message": trace.error(f"{txnId} does not exist in cache")}, 500
                    if len(txns) == 1:
                        if not txns[0]['id'] == txnId:
                            warning = f"txn with id {txnId} does not exist for {database} {table}"
                            return {'warning': trace.warning(warning)}, 500
                        # txnId is only value inside
                        tx = txns[0]
                        break
                    # multiple pending txns - need to check timestamp to verify if this txn can be commited yet
                    txns = sorted(txns, key=lambda txn: txn['timestamp'])
                    for ind, txn in enumerate(txns):
                        if txn['id'] == txnId:
                            if ind == 0:
                                tx = txns[0]
                                break
                            if waitTime > txnMaxWaitTimeInSec:
                                warning = f"timeout of {waitTime} reached while waiting to commit {txnId} for {database} {table}, waiting on {txns[:ind]}"
                                trace.warning(warning)
                                trace.warning(f"removing txn with id {txns[0]['id']} maxWaitTime of {txnMaxWaitTimeInSec} reached")
                                cache.delete(where={'id': txns[0]['id']})
                                break
                            break
                    if tx == None:
                        trace.warning(f"txnId {txnId} is behind txns {txns[:ind]} - waiting {waitTime} to retry")
                        time.sleep(waitInterval)
                        waitTime+=waitInterval 
                        # waitInterval scales up to txnMaxWaitIntervalInSec
                        waitInterval+=waitInterval 
                        if waitInterval >= txnMaxWaitIntervalInSec:
                            waitInterval = txnMaxWaitIntervalInSec
                        continue
                    break
                # Should not have broken out of loop here without a tx
                if tx == None:
                    trace.error("tx is None, this should not hppen")
                    return {"error": "tx was none"}, 500
                tx = cache.select('type','txn',
                        where={'id': txnId})[0]
                try:
                    r, rc = server.actions[tx['type']](database, table, tx['txn'])
                    trace.warning(f"##cache {action} response {r} rc {rc}")
                except Exception as e:
                    r, rc = trace.exception(f"Exception when performing cache {action}"), 500
                
                delTxn = cache.delete(
                    where={'id': txnId}
                )
                if rc == 200:
                    # update last txn id
                    setParams = {
                        'set': {
                            'lastTxnUuid': txnId,
                            'lastModTime': float(time.time())
                            },
                        'where': {
                            'tableName': table
                        }
                    }
                    server.data['cluster'].tables['pyql'].update(
                            **setParams['set'],
                            where=setParams['where']
                    )
                return {"message": r, "status": rc}, rc
            if action == 'cancel':
                delTxn = cache.delete(
                    where={'id': txnId}
                )
                return {'deleted': txnId}, 200
    @server.route('/db/<database>/cache/<table>/<action>/<txuuid>', methods=['POST'])
    @server.is_authenticated('local')
    def cache_action(database, table, action,txuuid):
        transaction = request.get_json()
        server.data[database].tables['cache'].insert(**{
            'id': txuuid,
            'tableName': table,
            'type': action,
            'timestamp': transaction['time'],
            'txn': transaction['txn']
        })
        return {"txn": txuuid}, 200