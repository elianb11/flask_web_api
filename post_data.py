import mysql.connector
from mysql.connector import Error
import pandas as pd
from request_data import *

def getConnectionFromServer():
    return mysql.connector.connect(
        host='ds50-mysql-do-user-9644544-0.b.db.ondigitalocean.com',
        database='ds50',
        user='ds50',
        password='AVNS_4ybSd0CoPKnCL5F',
        port = '25060')

def postUser(user):
    new_user_id = getDynamicNewUserID()
    connection = getConnectionFromServer()
    mycursor = connection.cursor()
    sql = """INSERT INTO
            USER
        VALUES (
            """+ new_user_id +""",
            '"""+ user['first_name'] +"""',
            '"""+ user['last_name'] +"""',
            '"""+ user['username'] +"""',
            '"""+ user['password'] +"""',
            '"""+ user['mail'] +"""',
            '"""+ user['address'] +"""',
            CURDATE(),
            """+ user['first_fav_category'] +""",
            """+ user['second_fav_category'] +""",
            """+ user['third_fav_category'] +"""
        )
        ;"""
    mycursor.execute(sql)
    connection.commit()
    return new_user_id

def postInteraction(interaction):
    connection = getConnectionFromServer()
    mycursor = connection.cursor()
    sql = """INSERT INTO
            INTERACTION
        VALUES (
            """+ interaction['user_id'] +""",
            """+ interaction['book_id'] +""",
            TRUE,
            """+ interaction['rating'] +""",
            '"""+ interaction['review_text'] +"""',
            CURDATE()
        )
        ;"""
    mycursor.execute(sql)
    connection.commit()
    return interaction

