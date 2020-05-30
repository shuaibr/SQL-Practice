
from asgn04 import table_info
from connect import Connect


# Connect to the DCRIS database with an option file
conn = Connect("dcris.ini")
rows = table_info(conn, table_schema='dcris', table_name=None)
for row in rows:
    print(row)
# Close the cursor and connection
conn.close()