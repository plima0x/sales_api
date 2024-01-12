import db
from psycopg import DatabaseError, ProgrammingError

try:
    if db.create_tables():
        if db.create_constraints():
            db.populate_all_tables()
    
except ProgrammingError as pge:
    print(f"[!] A programming error occurred while creating schema: {pge}")

except DatabaseError as dbe:
    print(f"[!] A Database error occurred while creating schema: {dbe}")

