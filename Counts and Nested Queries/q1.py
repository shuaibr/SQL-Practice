
from asgn03 import pub_counts_all
from connect import Connect


# Constants
conn = Connect("dcris.ini")

v1 = 90


if v1 == None:
    rows = pub_counts_all(conn)

else:
    rows = pub_counts_all(conn, member_id=v1)

for row in rows:
    print(row)
