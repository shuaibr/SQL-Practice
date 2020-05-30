
def keyword_table(conn, keyword_id=None):
    """
    -------------------------------------------------------
    Queries the keyword table.
    Use: rows = keyword_table(conn)
    Use: rows = keyword_table(conn, keyword_id=v)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        keyword_id - a keyword ID number (int)
    Postconditions:
        returns:
        rows - a list with the contents of the keyword table;
        the entire table if keyword_id is None, else the row 
        matching keyword_id (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()

    if keyword_id is None:
        sql = "SELECT * FROM keyword"
        cursor.execute(sql)
    else:
        sql = "SELECT * FROM keyword WHERE keyword_id = %s"
        params = (keyword_id,)
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows


def pub_table(conn, member_id=None, pub_type_id=None):
    """
    -------------------------------------------------------
    Queries the pub table.
    Use: rows = pub_table(conn)
    Use: rows = pub_table(conn, member_id=v1)
    Use: rows = pub_table(conn, pub_type_id=v2)
    Use: rows = pub_table(conn, member_id=v1, pub_type_id=v2)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        member_id - a member ID number (int)
        pub_type_id - a publication type (str)
    Postconditions:
        returns:
        rows - a list with the contents of the pub table;
        the entire table if member_id and pub_type_id are None,
        else rows matching member_id and pub_type_id 
        if given (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()

    if member_id is None and pub_type_id is None:
        sql = "SELECT * FROM pub"
        cursor.execute(sql)
    elif member_id is None:
        sql = "SELECT * FROM pub WHERE pub_type_id = %s"
        params = (pub_type_id,)
        cursor.execute(sql, params)
    elif pub_type_id is None:
        sql = "SELECT * FROM pub WHERE member_id = %s"
        params = (member_id,)
        cursor.execute(sql, params)
    else:
        sql = "SELECT * FROM pub WHERE member_id = %s AND pub_type_id = %s"
        params = (member_id, pub_type_id)
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows


def member_expertise(conn, member_id=None, keyword_id=None):
    """
    -------------------------------------------------------
    Queries the v_member_keyword view.
    Use: rows = member_expertise(conn)
    Use: rows = member_expertise(conn, member_id=v1)
    Use: rows = member_expertise(conn, keyword_id=v2)
    Use: rows = member_expertise(conn, member_id=v1, keyword_id=v2)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        member_id - a member ID number (int)
        keyword_id - a keyword ID number (int)
    Postconditions:
        returns:
        rows - a list with the last name, first name, and keyword
            description of the v_member_keyword view:
        the entire view if member_id and keyword_id are None, 
            sorted by last name, first name, keyword description
        rows matching member_id if keyword_id is None:
            sorted by last name, first name, keyword description
        rows matching keyword_id if member_id is None:
            sorted by keyword description, last name, first name
        otherwise rows unsorted
        if given (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()

    if member_id is None and keyword_id is None:
        sql = """SELECT last_name, first_name, k_desc FROM v_member_keyword 
        ORDER BY last_name, first_name, k_desc"""
        cursor.execute(sql)
    elif member_id is None:
        sql = """SELECT last_name, first_name, k_desc FROM v_member_keyword 
        WHERE keyword_id = %s ORDER BY k_desc, last_name, first_name"""
        params = (keyword_id,)
        cursor.execute(sql, params)
    elif keyword_id is None:
        sql = """SELECT last_name, first_name, k_desc FROM v_member_keyword 
        WHERE member_id = %s ORDER BY last_name, first_name, k_desc"""
        params = (member_id,)
        cursor.execute(sql, params)
    else:
        sql = """SELECT last_name, first_name, k_desc FROM v_member_keyword 
        WHERE member_id = %s AND keyword_id = %s"""
        params = (member_id, keyword_id)
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows


def expertise(conn, keyword=None, supp_key=None):
    """
    -------------------------------------------------------
    Queries the v_keyword_supp_key view.
    Use: rows = expertise(conn)
    Use: rows = expertise(conn, keyword=v1)
    Use: rows = expertise(conn, supp_key=v2)
    Use: rows = expertise(conn, keyword=v1, supp_key=v2)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        keyword - a partial keyword description (str)
        supp_key - a partial supplementary description (str)
    Postconditions:
        returns:
        rows - a list with the keyword and supplementary keyword descriptions
            of the v_keyword_supp_key view:
        the entire view if keyword and supp_key are None, 
            sorted by keyword description, supplementary keyword description
        rows containing keyword if supp_key is None:
            sorted by keyword description, supplementary keyword description
        rows matching supp_key if keyword is None:
            sorted by supplementary keyword description, keyword description 
        otherwise rows
            sorted by keyword description, supplementary keyword description
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()

    if keyword is None and supp_key is None:
        sql = """SELECT k_desc, sk_desc FROM v_keyword_supp_key 
        ORDER BY k_desc, sk_desc"""
        cursor.execute(sql)
    elif keyword is None:
        sql = """SELECT sk_desc, k_desc FROM v_keyword_supp_key 
        WHERE sk_desc LIKE %s ORDER BY sk_desc, k_desc"""
        params = ("%{}%".format(supp_key),)
        cursor.execute(sql, params)
    elif supp_key is None:
        sql = """SELECT k_desc, sk_desc FROM v_keyword_supp_key 
        WHERE k_desc LIKE %s ORDER BY k_desc, sk_desc"""
        params = ("%{}%".format(keyword),)
        cursor.execute(sql, params)
    else:
        sql = """SELECT k_desc, sk_desc FROM v_keyword_supp_key 
        WHERE k_desc LIKE %s AND sk_desc LIKE %s
        ORDER BY k_desc, sk_desc"""
        params = ("%{}%".format(keyword), "%{}%".format(supp_key))
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows
