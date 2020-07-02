
def db_attach(server):
    db = server.data['cluster']
    db.create_table(
       'auth', [
           ('id', str, 'UNIQUE NOT NULL'), 
           ('username', str, 'UNIQUE'),
           ('email', str, 'UNIQUE'),
           ('type', str), # admin / service / user
           ('password', str),
           ('parent', str) # uuid of parent, if service account or sub user account
        ],
        'id'
    )
    pass # Enter db.create_table statement here