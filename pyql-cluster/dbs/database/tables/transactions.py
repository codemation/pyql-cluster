
def db_attach(server):
    db = server.data['cluster']
    # 'endpoint': tb['endpoints'][tbEndpoint]['uuid'],
    # 'txnUuid': requestUuid,
    # 'timestamp': transTime,
    # 'txn': {action: requestData }
    
    db.create_table(
       'transactions', 
       [
           ('endpoint', str), 
           ('uuid', str),
           ('table', str), 
           ('cluster', str),
           ('timestamp', float),
           ('txn', str)
       ],
       'endpoint'
    )
    pass # Enter db.create_table statement here
            