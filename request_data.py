import mysql.connector
from mysql.connector import Error
import pandas as pd

def getConnectionFromServer():
    return mysql.connector.connect(
        host='ds50-mysql-do-user-9644544-0.b.db.ondigitalocean.com',
        database='ds50',
        user='ds50',
        password='AVNS_4ybSd0CoPKnCL5F',
        port = '25060')

def getFirst1000Books():
    connection = getConnectionFromServer()
    if connection.is_connected():
        df = pd.read_sql("SELECT * FROM BOOK LIMIT 1000;", connection)
        connection.close()
    return df.to_dict('records')

def getBookByBookId(book_id):
    connection = getConnectionFromServer()
    if connection.is_connected():
        df = pd.read_sql("SELECT * FROM BOOK WHERE BOOK_ID = "+ str(book_id) +";", connection)
        connection.close()
    return df.to_dict('records')[0]

def getAuthorByBookId(book_id):
    connection = getConnectionFromServer()
    if connection.is_connected():
        df = pd.read_sql(
            """SELECT
                A.*
            FROM
                AUTHOR A INNER JOIN WROTE W
                    ON A.AUTHOR_ID = W.AUTHOR_ID
            WHERE
                W.BOOK_ID = """+ str(book_id) +"""
            LIMIT 1;"""
            , connection)
        connection.close()
    return df.to_dict('records')[0]

def getFirst1000BooksByTag(tag_name):
    connection = getConnectionFromServer()
    if connection.is_connected():
        df = pd.read_sql(
            """SELECT
                B.*
            FROM
                BOOK B INNER JOIN TAG T
                    ON B.BOOK_ID = T.BOOK_ID
            WHERE
                T.TAG_NAME = '"""+ str(tag_name) +"""'
            LIMIT 1000;"""
            , connection)
    return df.to_dict('records')

def getBooksFromSameSerie(book_id):
    connection = getConnectionFromServer()
    if connection.is_connected():
        df = pd.read_sql(
            """SELECT
                B.*
            FROM
                BOOK B INNER JOIN CONTAINS C
                    ON B.BOOK_ID = C.BOOK_ID
            WHERE
                C.SERIES_ID = (
                    SELECT
                        S.SERIES_ID
                    FROM
                        SERIES S INNER JOIN CONTAINS C
                            ON S.SERIES_ID = C.SERIES_ID
                    WHERE
                        C.BOOK_ID = """+ str(book_id) +"""
                    LIMIT 1
                    )
            ;"""
            , connection)
    return df.to_dict('records')

def getAllTags():
    return [
        'fiction', 
        'fantasy', 
        'romance', 
        'classic', 
        'mystery', 
        'kindle', 
        'sci-fi', 
        'literature', 
        'horror', 
        'contemporary', 
        'adventure', 
        'historical', 
        'adult', 
        'paranormal', 
        'thriller', 
        'history', 
        'dystopia', 
        'audio', 
        'children', 
        'school', 
        'philosophy', 
        'novel', 
        'young'
        ]