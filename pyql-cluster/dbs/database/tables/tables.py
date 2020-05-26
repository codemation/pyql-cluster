def db_attach(server):
    import os
    db = server.data['cluster']
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
            ('id', str, 'NOT NULL'),
            ('name', str, 'NOT NULL'),
            ('database', str),
            ('cluster', str),
            ('config', str),
            ('consistency', bool),
            ('isPaused', bool)
       ],
        'id'
    )
    pass # Enter db.create_table statement here
            