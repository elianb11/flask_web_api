import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
import json

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
    df = pd.read_sql("SELECT * FROM BOOK LIMIT 1000;", connection)
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
            UPPER(B.TITLE) LIKE '%"""+ search_text.upper() +"""%'
        OR
            UPPER(A.NAME) LIKE '%"""+ search_text.upper() +"""%'
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
            BOOK B INNER JOIN TAGGED TGD
                ON B.BOOK_ID = TGD.BOOK_ID
            INNER JOIN TAG T
                ON TGD.TAG_ID = T.TAG_ID
        WHERE
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
        ORDER BY
            B.RATINGS_COUNT DESC
        ;"""
        , connection)
    df = fillDataframeNulls(df)
    return df.to_dict('records')

def getAllTags():
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            *
        FROM
            TAG
        ;"""
        , connection)
    df = fillDataframeNulls(df)
    return df.to_dict('records')

def getInteractionsByBookId(book_id):
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            I.*
            ,U.USERNAME
        FROM
            INTERACTION I INNER JOIN USER U
                ON U.USER_ID = I.USER_ID
        WHERE
            BOOK_ID = """+ str(book_id) +"""
        LIMIT 1000
        ;"""
        , connection)
    df["review_date"] = df["review_date"].astype(str)
    return df.to_dict('records')

def getUserByMail(mail):
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            *
        FROM
            USER
        WHERE
            mail = '"""+ mail +"""'
        ;"""
        , connection)
    df["sign_in_date"] = df["sign_in_date"].astype(str)
    return df.to_dict('records')[0]

def getStatsByBookId(book_id):
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
                RATING AS "rate"
                , COUNT(RATING) AS "total"
            FROM
                INTERACTION
            WHERE
                BOOK_ID = """+ str(book_id) +"""
            AND
                RATING > 0
            GROUP BY
                RATING
            ORDER BY
                1
            ;"""
        , connection)
    stats = {}
    stats["total_rates"] = int(df["total"].sum())
    stats["avg_rating"] = round((df["total"][0]*1 + df["total"][1]*2 + df["total"][2]*3 + df["total"][3]*4 + df["total"][4]*5) / stats["total_rates"], 1)
    stats["total_1"] = int(df["total"][0])
    stats["total_2"] = int(df["total"][1])
    stats["total_3"] = int(df["total"][2])
    stats["total_4"] = int(df["total"][3])
    stats["total_5"] = int(df["total"][4])
    return stats

def getDynamicNewUserID():
    connection = getConnectionFromServer()
    df = pd.read_sql(
        """SELECT
            user_id
        FROM
            USER
        ORDER BY
            user_id DESC
        LIMIT 1
        ;"""
        , connection)
    return str(df['user_id'][0] + 1)