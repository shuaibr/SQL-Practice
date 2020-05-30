
from joins import all_expertise
from connect import Connect

# Constants
conn = Connect("dcris.ini")

# member id
v1 = None

if v1 == None:
    rows = all_expertise(conn)
else:
    rows = all_expertise(conn, member_id=v1)

for row in rows:
    print(row)
