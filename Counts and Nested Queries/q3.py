
from asgn03 import keyword_count
from connect import Connect


# Constants
conn = Connect("dcris.ini")

# member id
v1 = 7

if v1 == None:
    rows = keyword_count(conn)
else:
    rows = keyword_count(conn, keyword_id=v1)


for row in rows:
    print(row)
