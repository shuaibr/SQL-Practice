from joins import publications
from connect import Connect


# Constants
conn = Connect("dcris.ini")

v1 = None
v2 = None


if v1 == None and v2 == None:
    rows = publications(conn)
elif v1 != None and v2 == None:
    rows = publications(conn, title=v1)

elif v1 == None and v2 != None:
    rows = publications(conn, pub_type_id=v2)
else:
    rows = publications(conn, title=v1, pub_type_id=v2)

for row in rows:
    print(row)
