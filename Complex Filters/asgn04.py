
from connect import Connect


def table_info(conn, table_schema, table_name=None):
    """
    -------------------------------------------------------
    Queries information_schema.TABLES for metadata.
    Use: rows = table_info(conn, table_schema)
    Use: rows = table_info(conn, table_schema, table_name=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        table_schema - the database table schema (str)
        table_name - a DCRIS table name (str)
    Postconditions:
        returns
        rows - a list of data from the TABLE_NAME, TABLE_TYPE, TABLE_ROWS, 
        and TABLE_COMMENT fields. Order by TABLE_NAME.
        If table_name is None, list all tables. (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()
    if(table_name is None):
        sql = """Select TABLE_NAME, TABLE_TYPE, TABLE_ROWS,TABLE_COMMENT 
        FROM information_schema.TABLES
        WHERE table_schema = %s
        ORDER BY TABLE_NAME
        """
        params = (table_schema,)
        cursor.execute(sql, params)
    elif(table_name is not None):
        sql = """Select TABLE_NAME, TABLE_TYPE, TABLE_ROWS,TABLE_COMMENT 
        FROM information_schema.TABLES
        WHERE table_schema = %s AND table_name = %s
        ORDER BY TABLE_NAME
        """
        params = (table_schema, table_name)
        cursor.execute(sql, params)

    rows = cursor.fetchall()

    cursor.close()
    return rows


def column_info(conn, table_schema, table_name=None):
    """
    -------------------------------------------------------
    Queries information_schema.COLUMNS for metadata.
    Use: rows = column_info(conn, table_schema)
    Use: rows = column_info(conn, table_schema, table_name=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        table_schema - the database table schema (str)
        table_name - a DCRIS table name (str)
    Postconditions:
        returns
        rows - a list of data from the TABLE_NAME, COLUMN_NAME, IS_NULLABLE, 
        and DATA_TYPE fields. Order by TABLE_NAME, COLUMN_NAME.
        If table_name is None, list all tables and their columns. (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()
    if(table_name is None):
        sql = """Select TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE 
        FROM information_schema.COLUMNS
        WHERE table_schema = %s
        ORDER BY TABLE_NAME, COLUMN_NAME
        """
        params = (table_schema,)
        cursor.execute(sql, params)
    elif(table_name is not None):
        sql = """Select TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE 
        FROM information_schema.COLUMNS
        WHERE table_schema = %s AND table_name = %s
        ORDER BY TABLE_NAME, COLUMN_NAME
        """
        params = (table_schema, table_name)
        cursor.execute(sql, params)

    rows = cursor.fetchall()

    cursor.close()
    return rows


def constraint_info(conn, table_schema, constraint_type=None):
    """
    -------------------------------------------------------
    Queries information_schema.TABLE_CONSTRAINTS for metadata.
    Use: rows = constraint_info(conn, table_schema)
    Use: rows = constraint_info(conn, table_schema, constraint_type=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        table_schema - the database table schema (str)
        constraint_type - a database constraint type (str)
    Postconditions:
        returns
        rows - a list of data from the CONSTRAINT_NAME, TABLE_NAME, 
        and CONSTRAINT_TYPE. Order by CONSTRAINT_NAME, TABLE_NAME.
        If constraint_type is None, list all constraints. (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()
    if(constraint_type is None):
        sql = """Select CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE 
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE table_schema = %s
        ORDER BY CONSTRAINT_NAME, TABLE_NAME
        """
        params = (table_schema,)
        cursor.execute(sql, params)
    elif(constraint_type is not None):
        sql = """Select CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE 
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE table_schema = %s AND constraint_type = %s
        ORDER BY CONSTRAINT_NAME, TABLE_NAME
        """
        params = (table_schema, constraint_type)
        cursor.execute(sql, params)

    rows = cursor.fetchall()

    cursor.close()
    return rows


def foreign_key_info(conn, constraint_schema, table_name=None, ref_table_name=None):
    """
    -------------------------------------------------------
    Queries information_schema.REFERENTIAL_CONSTRAINTS for metadata.
    Use: rows = foreign_key_info(conn, constraint_schema)
    Use: rows = foreign_key_info(conn, constraint_schema, table_name=v1)
    Use: rows = foreign_key_info(conn, constraint_schema, ref_table_name=v2)
    Use: rows = foreign_key_info(conn, constraint_schema, table_name=v1, ref_table_name=v2)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        constraint_schema - the database constraint schema (str)
        table_name - a DCRIS table name (str)
        ref_table_name - a DCRIS table name (str)
    Postconditions:
        returns
        rows - a list of data from the CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE, 
        TABLE_NAME, and REFERENCED_TABLE_NAME fields. Order by CONSTRAINT_NAME.
        If table_name and ref_table_name are None, list all rows. (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()
    if(table_name is None and ref_table_name is None):
        sql = """Select CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE,TABLE_NAME, REFERENCED_TABLE_NAME
        FROM information_schema.REFERENTIAL_CONSTRAINTS
        WHERE constraint_schema = %s
        ORDER BY CONSTRAINT_NAME
        """
        params = (constraint_schema,)
        cursor.execute(sql, params)
    elif(table_name is not None and ref_table_name is None):
        sql = """Select CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE,TABLE_NAME, REFERENCED_TABLE_NAME
        FROM information_schema.REFERENTIAL_CONSTRAINTS
        WHERE constraint_schema = %s AND table_name = %s
        ORDER BY CONSTRAINT_NAME
        """

        params = (constraint_schema, table_name)
        cursor.execute(sql, params)
    elif(table_name is None and ref_table_name is not None):
        sql = """Select CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE,TABLE_NAME, REFERENCED_TABLE_NAME
        FROM information_schema.REFERENTIAL_CONSTRAINTS
        WHERE constraint_schema = %s AND referenced_table_name = %s
        ORDER BY CONSTRAINT_NAME
        """

        params = (constraint_schema, ref_table_name)
        cursor.execute(sql, params)
    elif(table_name is not None and ref_table_name is not None):
        sql = """Select CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE,TABLE_NAME, REFERENCED_TABLE_NAME
        FROM information_schema.REFERENTIAL_CONSTRAINTS
        WHERE constraint_schema = %s AND table_name = %s AND referenced_table_name = %s
        ORDER BY CONSTRAINT_NAME
        """

        params = (constraint_schema, table_name, ref_table_name)
        cursor.execute(sql, params)

    rows = cursor.fetchall()

    cursor.close()
    return rows


def key_info(conn, constraint_schema, table_name=None, ref_table_name=None):
    """
    -------------------------------------------------------
    Queries information_schema.KEY_COLUMN_USAGE for metadata.
    Use: rows = key_info(conn, constraint_schema)
    Use: rows = key_info(conn, constraint_schema, table_name=v1)
    Use: rows = key_info(conn, constraint_schema, ref_table_name=v2)
    Use: rows = key_info(conn, constraint_schema, table_name=v1, ref_table_name=v2)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        constraint_schema - the database constraint schema (str)
        table_name - a DCRIS table name (str)
        ref_table_name - a DCRIS table name (str)
    Postconditions:
        returns
        rows - a list of data from the CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, 
        REFERENCED_TABLE_NAME, and REFERENCED_COLUMN_NAME fields. Order by 
        TABLE_NAME, COLUMN_NAME. 
        If table_name and ref_table_name are None, list all rows. (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()
    if(table_name is None and ref_table_name is None):
        sql = """Select CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE constraint_schema = %s
        ORDER BY TABLE_NAME, COLUMN_NAME
        """
        params = (constraint_schema,)
        cursor.execute(sql, params)
    elif(table_name is not None and ref_table_name is None):
        sql = """Select CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE constraint_schema = %s AND table_name = %s
        ORDER BY TABLE_NAME, COLUMN_NAME
        """

        params = (constraint_schema, table_name)
        cursor.execute(sql, params)
    elif(table_name is None and ref_table_name is not None):
        sql = """Select CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE constraint_schema = %s AND referenced_table_name = %s
        ORDER BY TABLE_NAME, COLUMN_NAME
        """

        params = (constraint_schema, ref_table_name)
        cursor.execute(sql, params)
    elif(table_name is not None and ref_table_name is not None):
        sql = """Select CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE constraint_schema = %s AND table_name = %s AND referenced_table_name = %s
        ORDER BY TABLE_NAME, COLUMN_NAME
        """

        params = (constraint_schema, table_name, ref_table_name)
        cursor.execute(sql, params)

    rows = cursor.fetchall()

    cursor.close()
    return rows
