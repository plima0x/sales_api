import db

if db.create_tables():
    if db.create_constraints():
        db.populate_all_tables()
