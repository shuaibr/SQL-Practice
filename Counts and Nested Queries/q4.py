
from asgn03 import keyword_member_count
from connect import Connect

# Constants
conn = Connect("dcris.ini")

# member id
v1 = 13

if v1 == None:
    rows = keyword_member_count(conn)
else:
    rows = keyword_member_count(conn, keyword_id=v1)

for row in rows:
    print(row)
