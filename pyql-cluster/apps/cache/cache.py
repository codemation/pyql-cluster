def run(server):
    from flask import request
    import os, uuid, time, json
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

    @server.route('/db/<database>/cache/<table>/txn/<action>', methods=['POST'])
    def cache_txn_manage(database, table, action):
        """
            method for managing txns - canceling / commiting
        """
        transaction = request.get_json()
        print(f"cache_txn_manage - received {action} requeest for {transaction}")
        print(type(transaction))
        if database in server.cache and table in server.cache[database]['tables']:
            tb = server.data[database].tables[table]
            cache = server.cache[database]['tables'][table]
            if 'txn' in transaction:
                txnId = transaction['txn']
                if txnId in cache['txns']:
                    if action == 'commit':
                        cachedAction = cache['txns'][txnId]['type']
                        cachedTxn = cache['txns'][txnId]['txn']
                        print(f"cache_txn_manage - commiting {cachedTxn} type {type(cachedTxn)}")
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
                        print(f"cache commit response {response} {rc}")
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


    @server.route('/db/<database>/cache/<table>/<action>/<txuuid>', methods=['POST'])
    def cache_action(database, table, action,txuuid):
        transaction = request.get_json()
        print(f"#cache_action {action} {transaction}")
        if database in server.cache and table in server.cache[database]['tables']:
            tb = server.data[database].tables[table]
            cache = server.cache[database]['tables'][table]
            txnUuid = txuuid
            cache['txns'][txnUuid] = {
                "type": action,
                "txn": transaction
            }
            return {"txn": txnUuid}