def db_attach(server):
    import os
    db = server.data[os.environ['DB_NAME']]
    # Example 
    # db.create_table(
    #    'users', # table-name
    #     [
    #        ('userid', int, 'AUTOINCREMENT'),
    #        ('username', str, 'UNIQUE NOT NULL'),
    #        ('email', str, 'NOT NULL'),
    #        ('join_date', str),
    #        ('last_login', str),
    #     ],
    # 'userid' # Primary Key
    # )
    #UNCOMMENT Below to create
    #
    db.create_table(
       'tables', [
           ('name': str), 
           ('database': str), 
           ('cluster': str),
           ('mirrored': bool),
           ('consistency': str),
        'name'
    )
    pass # Enter db.create_table statement here
            