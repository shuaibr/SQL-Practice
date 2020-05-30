
from asgn04 import constraint_info
from connect import Connect


# Connect to the DCRIS database with an option file
conn = Connect("dcris.ini")
rows = constraint_info(
    conn, table_schema='dcris', constraint_type=None)
for row in rows:
    print(row)
# Close the cursor and connection
conn.close()
