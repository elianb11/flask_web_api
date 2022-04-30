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

def fillDataframeNulls(df: pd.DataFrame):
    for col in df.columns:
        if df[col].dtype == object:
            df[col].fillna('', inplace=True)
        else:
            df[col].fillna(0, inplace=True)
    return df

def getFirst1000Books():
    connection = getConnectionFromServer()
    df = pd.read_sql("SELECT * FROM BOOK WHERE LANGUAGE_CODE LIKE 'en%' LIMIT 1000;", connection)
    df = fillDataframeNulls(df)
    return df.to_dict('records')

def getFirst1000BooksBySearch(search_text):
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            DISTINCT B.*
        FROM
            BOOK B INNER JOIN WROTE W
                ON B.BOOK_ID = W.BOOK_ID
            INNER JOIN AUTHOR A
                ON A.AUTHOR_ID = W.AUTHOR_ID
        WHERE
            B.LANGUAGE_CODE LIKE 'en%'
        AND (
                UPPER(B.TITLE) LIKE '%"""+ search_text.upper() +"""%'
            OR
                UPPER(A.NAME) LIKE '%"""+ search_text.upper() +"""%'
            )
        ORDER BY
            B.RATINGS_COUNT DESC
        LIMIT 1000;"""
        , connection)
    df = fillDataframeNulls(df)
    return df.to_dict('records')

def getBookByBookId(book_id):
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            B.*
            ,A.NAME as "author_name"
            FROM
                BOOK B INNER JOIN WROTE W
                    ON B.BOOK_ID = W.BOOK_ID
                INNER JOIN AUTHOR A
                    ON A.AUTHOR_ID = W.AUTHOR_ID
            WHERE
                B.BOOK_ID = """+ str(book_id) +"""
            LIMIT 1;"""
        , connection)
    df = fillDataframeNulls(df)
    return df.to_dict('records')[0]

def getAuthorByBookId(book_id):
    connection = getConnectionFromServer()
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
    df = fillDataframeNulls(df)
    return df.to_dict('records')[0]

def getFirst1000BooksByTag(tag_name):
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            DISTINCT B.*
        FROM
            BOOK B INNER JOIN TAG T
                ON B.BOOK_ID = T.BOOK_ID
        WHERE
            B.LANGUAGE_CODE LIKE 'en%'
        AND
            T.TAG_NAME = '"""+ str(tag_name) +"""'
        ORDER BY
            B.RATINGS_COUNT DESC
        LIMIT 1000;"""
        , connection)
    df = fillDataframeNulls(df)
    return df.to_dict('records')

def getBooksFromSameSerie(book_id):
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            DISTINCT B.*
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
            AND
                B.LANGUAGE_CODE LIKE 'en%'
        ORDER BY
            B.RATINGS_COUNT DESC
        ;"""
        , connection)
    df = fillDataframeNulls(df)
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