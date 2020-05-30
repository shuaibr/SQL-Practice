
# Imports
from connect import Connect


def pub_counts_all(conn, member_id=None):
    """
    -------------------------------------------------------
    Queries the pub and member tables.
    Use: rows = pub_counts(conn)
    Use: rows = pub_counts(conn, member_id=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        member_id - a member ID number (int)
    Postconditions:
        returns
        rows - a list with a member's last name, a member's first
        name, and the number of publications of each type. Name these
        three fields "articles", "papers", and "books". List the results
        as appropriate in order by member last name and first name.
        If member_id is None, list all members. (list of ?)
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    # member id doesn't exist
    sql2 =  """SELECT last_name, first_name,
            (SELECT COUNT(*) FROM pub AS p INNER JOIN member AS m ON m.member_id = p.member_id WHERE pub_type_id = 'a' AND m.member_id = a.member_id)As article ,
            (SELECT COUNT(*) FROM pub AS p INNER JOIN member AS m ON m.member_id = p.member_id WHERE pub_type_id = 'p' AND m.member_id = a.member_id)As paper,
            (SELECT COUNT(*) FROM pub AS p INNER JOIN member AS m ON m.member_id = p.member_id WHERE pub_type_id = 'b' AND m.member_id = a.member_id)As book
            FROM 
            (SELECT last_name,first_name,m.member_id FROM pub AS p JOIN member AS m ON p.member_id = m.member_id GROUP BY member_id ORDER BY last_name,first_name) a"""

    # member id exists
    sql1 = """SELECT last_name, first_name,
            (SELECT COUNT(*) FROM pub AS p INNER JOIN member AS m ON m.member_id = p.member_id WHERE pub_type_id = 'a' AND m.member_id = a.member_id)As article ,
            (SELECT COUNT(*) FROM pub AS p INNER JOIN member AS m ON m.member_id = p.member_id WHERE pub_type_id = 'p' AND m.member_id = a.member_id)As paper,
            (SELECT COUNT(*) FROM pub AS p INNER JOIN member AS m ON m.member_id = p.member_id WHERE pub_type_id = 'b' AND m.member_id = a.member_id)As book
            FROM 
            (SELECT last_name,first_name,m.member_id FROM pub AS p JOIN member AS m ON p.member_id = m.member_id WHERE m.member_id = %s GROUP BY member_id) a"""

    if member_id != None:
        params = (member_id, )
        cursor.execute(sql1, params)

    else:
        cursor.execute(sql2, )

    rows = cursor.fetchall()
    cursor.close()
    return rows


def expertise_count(conn, member_id=None):
    """
    -------------------------------------------------------
    Use: rows = expertise_count(conn)
    Use: rows = expertise_count(conn, member_id=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        member_id - a member ID number (int)
    Postconditions:
        returns
        rows - a list with a member's last name, a member's first
        name, and the number of keywords and supplementary keywords
        for the member. Name these fields "keywords" and "supp_keys".
        List the results as appropriate in order by member last 
        name and first name. If member_id is None, list all members.
        (list of ?)
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    # member id doesn't exist
    sql2 =  """SELECT last_name, first_name,
            (SELECT COUNT(*) FROM member AS p INNER JOIN member_keyword AS m ON m.member_id = p.member_id WHERE m.member_id = a.member_id)As keywords ,
            (SELECT COUNT(*) FROM member AS p INNER JOIN member_supp_key AS m ON m.member_id = p.member_id WHERE m.member_id = a.member_id)As supp_keys
            FROM 
            (SELECT last_name,first_name,member_id FROM member ORDER BY last_name,first_name) a"""

    # member id exists
    sql1 = """SELECT last_name, first_name,
            (SELECT COUNT(*) FROM member AS p INNER JOIN member_keyword AS m ON m.member_id = p.member_id WHERE m.member_id = a.member_id)As keywords ,
            (SELECT COUNT(*) FROM member AS p INNER JOIN member_supp_key AS m ON m.member_id = p.member_id WHERE m.member_id = a.member_id)As supp_keys
            FROM 
            (SELECT last_name,first_name,member_id FROM member WHERE member_id = %s ORDER BY last_name,first_name) a"""

    if member_id != None:
        params = (member_id, )
        cursor.execute(sql1, params)

    else:
        cursor.execute(sql2, )

    rows = cursor.fetchall()
    cursor.close()
    return rows


def keyword_count(conn, keyword_id=None):
    """
    -------------------------------------------------------
    Use: rows = keyword_count(conn)
    Use: rows = keyword_count(conn, keyword_id=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        keyword_id - a keyword ID number (int)
    Postconditions:
        returns
        rows - a list with a keyword's description and the number of
        supplementary keywords that belong to it. Name the second field
        "supp_key_count".
        List the results as appropriate in order by keyword description. 
        If keyword_id is None, list all keywords. (list of ?)
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    # keyword id doesn't exist
    sql2 =  """SELECT k_desc,
            (SELECT COUNT(*) FROM supp_key AS p JOIN keyword AS k ON p.keyword_id = k.keyword_id WHERE k.keyword_id = a.keyword_id)As supp_key_count
            FROM 
            (SELECT k_desc, keyword_id FROM keyword ORDER BY k_desc) a"""

    # keyword id exists
    sql1 = """SELECT k_desc,
            (SELECT COUNT(*) FROM supp_key AS p JOIN keyword AS k ON p.keyword_id = k.keyword_id WHERE k.keyword_id = a.keyword_id)As supp_key_count
            FROM 
            (SELECT k_desc, keyword_id FROM keyword WHERE keyword_id = %s ORDER BY k_desc) a"""

    if keyword_id != None:
        params = (keyword_id, )
        cursor.execute(sql1, params)

    else:
        cursor.execute(sql2, )

    rows = cursor.fetchall()
    cursor.close()
    return rows


def keyword_member_count(conn, keyword_id=None):
    """
    -------------------------------------------------------
    Use: rows = keyword_member_count(conn)
    Use: rows = keyword_member_count(conn, keyword_id=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        keyword_id - a keyword ID number (int)
    Postconditions:
        returns
        rows - a list with a keyword's description and the number of
        members that have it. Name the second field
        "member_count".
        List the results as appropriate in order by keyword description. 
        If keyword_id is None, list all keywords. (list of ?)
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    # keyword id doesn't exist
    sql2 =  """SELECT k_desc,
            (SELECT COUNT(*) FROM member_keyword AS p JOIN keyword AS k ON p.keyword_id = k.keyword_id WHERE k.keyword_id = a.keyword_id)As member_count
            FROM 
            (SELECT k_desc, keyword_id FROM keyword ORDER BY k_desc) a"""

    # keyword id exists
    sql1 = """SELECT k_desc,
            (SELECT COUNT(*) FROM member_keyword AS p JOIN keyword AS k ON p.keyword_id = k.keyword_id WHERE k.keyword_id = a.keyword_id)As member_count
            FROM 
            (SELECT k_desc, keyword_id FROM keyword WHERE keyword_id = %s ORDER BY k_desc) a"""

    if keyword_id != None:
        params = (keyword_id, )
        cursor.execute(sql1, params)

    else:
        cursor.execute(sql2, )

    rows = cursor.fetchall()
    cursor.close()
    return rows


def supp_key_member_count(conn, supp_key_id=None):
    """
    -------------------------------------------------------
    Use: rows = supp_key_member_count(conn)
    Use: rows = supp_key_member_count(conn, supp_key_id=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        supp_key_id - a supp_key ID number (int)
    Postconditions:
        returns
        rows - a list with a keyword's description, a supplementary
        keyword description, and the number of members that have it. 
        Name the last field "member_count".
        List the results as appropriate in order by keyword description
        and then supplementary keyword description.
        If supp_key_id is None, list all keywords and supplementary
        keywords. (list of ?)
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    # keyword id doesn't exist
    sql2 =  """SELECT k_desc, sk_desc,
            (SELECT COUNT(*) FROM member_supp_key AS p JOIN supp_key AS k ON p.supp_key_id = k.supp_key_id WHERE k.supp_key_id = a.supp_key_id)As member_count
            FROM 
            (SELECT k_desc, sk_desc, supp_key_id FROM keyword AS m JOIN supp_key AS sk ON m.keyword_id = sk.keyword_id ORDER BY k_desc, sk_desc) a"""

    # keyword id exists
    sql1 = """SELECT k_desc, sk_desc,
            (SELECT COUNT(*) FROM member_supp_key AS p JOIN supp_key AS k ON p.supp_key_id = k.supp_key_id WHERE k.supp_key_id = a.supp_key_id)As member_count
            FROM 
            (SELECT k_desc, sk_desc, supp_key_id FROM keyword AS m JOIN supp_key AS sk ON m.keyword_id = sk.keyword_id WHERE supp_key_id = %s ORDER BY k_desc, sk_desc) a"""

    if supp_key_id != None:
        params = (supp_key_id, )
        cursor.execute(sql1, params)

    else:
        cursor.execute(sql2, )

    rows = cursor.fetchall()
    cursor.close()
    return rows
