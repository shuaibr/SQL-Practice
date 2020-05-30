# Imports
from connect import Connect
 

# Constants
def publications(conn, title=None, pub_type_id=None):
    """
    -------------------------------------------------------
    Queries the pub table.
    Use: rows = publications(conn)
    Use: rows = publications(conn, title=v1)
    Use: rows = publications(conn, pub_type_id=v2)
    Use: rows = publications(conn, title=v1, pub_type_id=v2)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        title - a partial title (str)
        pub_type_id - a publication type (str)
    Postconditions:
        returns:
        rows - a list with a members's last name, a member's first
                name, the title of a publication, and the full publication
                type (i.e. 'article' rather than 'a';
        the entire table if title and pub_type_id are None,
        else rows matching the partial title and pub_type_id 
        if given sorted by last name, first name, title (list of ?)
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    # both match - else rows matching the partial title and pub_type_id if
    # given sorted by last name, first name, title (list of ?)

    # only p_type exists
    sql1 =  """SELECT m.last_name, m.first_name, p_title, pt.pt_desc
            FROM pub AS p
            INNER JOIN member AS m 
            ON m.member_id = p.member_id
            INNER JOIN pub_type AS pt
            ON pt.pub_type_id = p.pub_type_id
            WHERE p.pub_type_id LIKE %s
            ORDER BY m.last_name, m.first_name ASC"""

    # only title exists
    sql2 = """SELECT m.last_name, m.first_name, p_title, pt.pt_desc
            FROM pub AS p
            INNER JOIN member AS m 
            ON m.member_id = p.member_id
            INNER JOIN pub_type AS pt
            ON pt.pub_type_id = p.pub_type_id
            WHERE p_title LIKE %s
            ORDER BY m.last_name, m.first_name, p_title ASC"""

    # the contents of table if title and pub_type_id are not None
    sql = """SELECT m.last_name, m.first_name, p_title, pt.pt_desc
            FROM pub AS p
            INNER JOIN member AS m 
            ON m.member_id = p.member_id
            INNER JOIN pub_type AS pt
            ON pt.pub_type_id = p.pub_type_id
            WHERE p_title LIKE %s AND p.pub_type_id LIKE %s 
            ORDER BY m.last_name, m.first_name, p_title ASC"""

    if title == None and pub_type_id == None:
        sql = """SELECT m.last_name, m.first_name, p_title, pt.pt_desc
            FROM pub AS p
            INNER JOIN member AS m 
            ON m.member_id = p.member_id
            INNER JOIN pub_type AS pt
            ON pt.pub_type_id = p.pub_type_id 
            ORDER BY m.last_name, m.first_name ASC"""
        cursor.execute(sql,)
    elif title == None:
        params = ('%' + pub_type_id + '%',)
        cursor.execute(sql1, params)

    elif pub_type_id == None:
        params = ('%' + title + '%',)
        cursor.execute(sql2, params)
    else:
        params = ('%' + title + '%', '%' + pub_type_id + '%')
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows


def pub_counts(conn, member_id, pub_type_id=None):
    """
    -------------------------------------------------------
    Queries the pub table.
    Use: rows = pub_counts(conn, member_id=v1)
    Use: rows = pub_counts(conn, member_id=v1, pub_type_id=v2)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        member_id - a member ID number (int)
        pub_type_id - a publication type (str)
    Postconditions:
        returns:
        rows - a list with a members's last name, a member's first
        name, and the number of publications of type pub_type
        if given, if not, the number of all their publications (list of ?)
    -------------------------------------------------------
    """
    cursor = conn.get_cursor()

    sql = """SELECT last_name, first_name, 
        COUNT(pub_type_id)
        FROM pub AS p
        INNER JOIN member AS m
        ON m.member_id = p.member_id
        WHERE m.member_id = %s AND p.pub_type_id = %s"""

    sql2 = """SELECT last_name, first_name, 
        COUNT(pub_type_id)
        FROM pub AS p
        INNER JOIN member AS m
        ON m.member_id = p.member_id
        WHERE m.member_id = %s"""


    if member_id != None and pub_type_id == None:
        params = (member_id,)
        cursor.execute(sql2, params)

    else:
        params = (member_id, pub_type_id)
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows


def member_expertise_count(conn, member_id=None):
    """
    -------------------------------------------------------
    Use: rows = member_expertise_count(conn)
    Use: rows = member_expertise_count(conn, member_id=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        member_id - a member ID number (int)
    Postconditions:
        returns:
        rows - a list with a members's last name, a member's first
        name, and the count of the number of expertises they
            hold (i.e. keywords)
        all records member_id is None, sorted by last name, first name
        (list of ?)
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    sql = """SELECT m.last_name, m.first_name, 
        COUNT(p.keyword_id)
        FROM member_keyword AS p
        INNER JOIN member AS m
        ON m.member_id = p.member_id
        WHERE p.member_id = %s
        ORDER BY last_name, first_name"""

    sql2 = """SELECT m.last_name, m.first_name,
        COUNT(p.keyword_id)
        FROM member_keyword AS p
        INNER JOIN member AS m
        ON m.member_id = p.member_id
        GROUP BY last_name, first_name"""

    if member_id == None:
        params = ()
        cursor.execute(sql2, )

    else:
        params = (member_id, )
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows


def all_expertise(conn, member_id=None):
    """
    -------------------------------------------------------
    Use: rows = all_expertise(conn)
    Use: rows = all_expertise(conn, member_id=v1)
    -------------------------------------------------------
    Preconditions:
        conn - a database connection (Connect)
        member_id - a member ID number (int)
    Postconditions:
        returns:
        rows - a list with a members's last name, a member's first
        name, a keyword descrption, and a supplementary keyword description
        all records if member_id is None, 
        sorted by last_name, first_name, keyword description, supplementary 
                keyword description
    -------------------------------------------------------
    """

    cursor = conn.get_cursor()

    sql = """SELECT m.last_name, m.first_name, kw.k_desc, sk.sk_desc
        FROM member_keyword AS p
        INNER JOIN member AS m
        ON m.member_id = p.member_id
        INNER JOIN keyword AS kw
        ON kw.keyword_id = p.keyword_id
        INNER JOIN supp_key AS sk
        ON sk.keyword_id = kw.keyword_id 
        WHERE p.member_id = %s
        ORDER BY last_name, first_name"""

    sql2 = """SELECT m.last_name, m.first_name, kw.k_desc, sk.sk_desc
        FROM member_keyword AS p
        INNER JOIN member AS m
        ON m.member_id = p.member_id
        INNER JOIN keyword AS kw
        ON kw.keyword_id = p.keyword_id
        INNER JOIN supp_key AS sk
        ON sk.keyword_id = kw.keyword_id 
        GROUP BY last_name, first_name"""

    if member_id == None:
        params = ()
        cursor.execute(sql2, )

    else:
        params = (member_id, )
        cursor.execute(sql, params)

    rows = cursor.fetchall()
    cursor.close()
    return rows
