def run(server):
    from flask import request
    import os, uuid, time, json
    log = server.log
    @server.route('/db/<database>/cache/<table>/txn/<action>', methods=['POST'])
    def cache_txn_manage(database, table, action):
        """
            method for managing txns - canceling / commiting
        """
        transaction = request.get_json()
        if 'txn' in transaction:
            txnId = transaction['txn']
            # Get transaction from cache db
            if action == 'commit':
                #commit txn
                txn = server.data[database].tables['cache'].select('*',
                    where={'id': txnId}
                )
                if len(txn) == 1:
                    tx = txn[0]
                    r, rc = server.actions[tx['type']](database, table, tx['txn'])
                    log.warning(f"##cache {action} response {r} rc {rc}")
                    if rc == 200:
                        delTxn = server.data[database].tables['cache'].delete(
                            where={'id': txnId}
                        )
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
                        server.data[database].tables['pyql'].update(
                                **setParams['set'],
                                where=setParams['where']
                        )  
                    return {"message": r, "status": rc}, rc
                warning = f"txn with id {txnId} does not exist or more than one was found {txn['data']} in {database} {table}"
                log.warning(warning)
                return {'warning': warning}, 400
            if action == 'cancel':
                delTxn = server.data[database].tables['cache'].delete(
                    where={'id': txnId}
                )
                return {'deleted': txnId}, 200

    @server.route('/db/<database>/cache/<table>/<action>/<txuuid>', methods=['POST'])
    def cache_action(database, table, action,txuuid):
        transaction = request.get_json()
        server.data[database].tables['cache'].insert(**{
            'id': txuuid,
            'type': action,
            'txn': transaction
        })
        return {"txn": txuuid}, 200