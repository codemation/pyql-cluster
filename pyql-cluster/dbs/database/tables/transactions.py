
def db_attach(server):
    db = server.data['cluster']
    # 'endpoint': tb['endpoints'][tbEndpoint]['uuid'],
    # 'txnUuid': requestUuid,
    # 'timestamp': transTime,
    # 'txn': {action: requestData }
    
    db.create_table(
       'transactions', 
       [
           ('txNumber', int, 'AUTOINCREMENT'),
           ('endpoint', str), 
           ('uuid', str),
           ('tableName', str), 
           ('cluster', str),
           ('timestamp', float),
           ('txn', str)
       ],
       'txNumber'
    )
    pass # Enter db.create_table statement here
            