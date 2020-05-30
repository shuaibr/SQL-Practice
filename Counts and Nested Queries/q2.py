
from asgn03 import expertise_count
from connect import Connect


# Constants
conn = Connect("dcris.ini")

v1 = 90


if v1 == None:
    rows = expertise_count(conn)

else:
    rows = expertise_count(conn, member_id=v1)

for row in rows:
    print(row)
