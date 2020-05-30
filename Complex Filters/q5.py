
from asgn04 import key_info
from connect import Connect


# Connect to the DCRIS database with an option file
conn = Connect("dcris.ini")
rows = key_info(
    conn, constraint_schema='dcris', table_name=None, ref_table_name=None)
for row in rows:
    print(row)
# Close the cursor and connection
conn.close()
