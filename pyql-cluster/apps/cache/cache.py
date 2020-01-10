def run(server):
    from flask import request
    import os, uuid, time, json
    log = server.log
    """ TODO - replaced by table model as in-memory is not easily shared by webserver workers(uWsgi)
    server.cache = {}

    @server.route('/db/<database>/cache/<table>/enable', methods=['POST'])
    def cache_enable(database, table):
        if database in server.data:
            if not database in server.cache:
                server.cache[database] = {
                    'tables': {}
                    }
            if not table in server.cache[database]['tables']:
                tb = server.data[database].tables[table]
                server.cache[database]['tables'][table] = {
                    "txns": {}
                }
    ## enable cache for current db tables ## 
    server.actions['cache_enable'] = cache_enable
    for database in server.data:
        for table in server.data[database].tables:
            cache_enable(database, table)
    """
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


        """ TODO - delete
        if database in server.cache and table in server.cache[database]['tables']:
            tb = server.data[database].tables[table]
            cache = server.cache[database]['tables'][table]
            if 'txn' in transaction:
                txnId = transaction['txn']
                if txnId in cache['txns']:
                    if action == 'commit':
                        cachedAction = cache['txns'][txnId]['type']
                        cachedTxn = cache['txns'][txnId]['txn']
                        response, rc = server.actions[cachedAction](database, table, cachedTxn)
                        if rc == 200:
                            del cache['txns'][txnId]
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
                        return {"message": response, "status": rc}, rc
                    elif action == 'cancel':
                        del cache['txns'][txnId]
                    else:
                        return {'message': f"{action} is not a valid action, use /commit or /cancel"}, 400
                else:
                    return {
                        'message': f"{txnId} is not valid transaction id for db {database} table {table}"
                        }, 400
        else:
            return {
                "message": f"DB {database} or table {table} is not valid, or cache is not enabled"
            }
        """

    @server.route('/db/<database>/cache/<table>/<action>/<txuuid>', methods=['POST'])
    def cache_action(database, table, action,txuuid):
        transaction = request.get_json()
        server.data[database].tables['cache'].insert(**{
            'id': txuuid,
            'type': action,
            'txn': transaction
        })
        return {"txn": txuuid}, 200

        """ TODO - delete
        if database in server.cache and table in server.cache[database]['tables']:
            tb = server.data[database].tables[table]
            cache = server.cache[database]['tables'][table]
            txnUuid = txuuid
            cache['txns'][txnUuid] = {
                "type": action,
                "txn": transaction
            }
            return {"txn": txnUuid}, 200
        """