
from joins import pub_counts
from connect import Connect


# Constants
conn = Connect("dcris.ini")

# member id
v1 = 3
# pub_type_id
v2 = None

if v1 != None and v2 == None:
    rows = pub_counts(conn, member_id=v1)
else:
    rows = pub_counts(conn, member_id=v1, pub_type_id=v2)

for row in rows:
    print(row)
