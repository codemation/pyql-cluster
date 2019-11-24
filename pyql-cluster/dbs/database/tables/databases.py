def db_attach(server):
    import os
    db = server.data[os.environ['DB_NAME']]
    db.create_table(
       'databases', [
           ('name', str), 
           ('cluster', str)
    )
    pass # Enter db.create_table statement here
            