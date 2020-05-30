
from joins import member_expertise_count
from connect import Connect


# Constants
conn = Connect("dcris.ini")

# member id
v1 = None

# if v1 == None:
#     rows = member_expertise_count(conn)
# else:
rows = member_expertise_count(conn, member_id=None)

for row in rows:
    print(row)
