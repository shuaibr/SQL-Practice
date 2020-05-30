
from asgn03 import supp_key_member_count
from connect import Connect

# Constants
conn = Connect("dcris.ini")

# member id
v1 = 70

if v1 == None:
    rows = supp_key_member_count(conn)
else:
    rows = supp_key_member_count(conn, supp_key_id=v1)

for row in rows:
    print(row)
