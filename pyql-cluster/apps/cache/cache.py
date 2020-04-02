def run(server):
    from flask import request
    import os, uuid, time, json
    log = server.log
    txnDefaultWaitInSec = 0.005 # default 5 ms
    txnMaxWaitTimeInSec = 0.125

    @server.route('/db/<database>/cache/<table>/txn/<action>', methods=['POST'])
    @server.is_authenticated('local')
    def cache_txn_manage(database, table, action, trans=None):
        """
            method for managing txns - canceling / commiting
        """
        cache = server.data[database].tables['cache']
        transaction = request.get_json() if trans == None else trans
        if 'txn' in transaction:
            txnId = transaction['txn']
            txnOrder = None
            tx=None
            waitTime = txnDefaultWaitInSec
            # Get transaction from cache db
            if action == 'commit':
                while True:
                    txns = cache.select('id','timestamp',
                        where={'tableName': table}
                    )
                    if not txnId in {tx['id'] for tx in txns}:
                        return {"message": f"{txnId} does not exist in cache"}, 400
                    if len(txns) == 1:
                        if not txns[0]['id'] == txnId:
                            warning = f"txn with id {txnId} does not exist for {database} {table}"
                            log.warning(warning)
                            return {'warning': warning}, 400
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
                                log.warning(warning)
                                log.warning(f"removing txn with id {txns[0]['id']} maxWaitTime of {txnMaxWaitTimeInSec} reached")
                                cache.delete(where={'id': txns[0]['id']})
                                break
                            break
                    if tx == None:
                        log.warning(f"txnId {txnId} is behind txns {txns[:ind]} - waiting {waitTime} to retry")
                        time.sleep(waitTime)
                        waitTime+=waitTime # wait time scales up
                        continue
                    break

                if tx == None:
                    log.error("tx is None, this should not hppen")
                    return {"error": "tx was none"}, 500
                tx = cache.select('type','txn',
                        where={'id': txnId})[0]
                try:
                    r, rc = server.actions[tx['type']](database, table, tx['txn'])
                    log.warning(f"##cache {action} response {r} rc {rc}")
                except Exception as e:
                    log.exception(e)
                    r, rc = repr(e), 400
                
                
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