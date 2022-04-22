from filtering import ContentBasedFiltering

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

def getReco(method='content', filter_base=None):
    connection = getConnectionFromServer()

    if method == 'content' and filter_base:
        filtering = ContentBasedFiltering()
        filtering.setFilterBase(filter_base)
        filtering.setConnection(connection)
        filtering.loadData()
        filtering.processData()
        reco = filtering.filter()
    
    if method == 'collab':
        #TODO Add collaborative filtering
        pass

    query = f"""
        SELECT
            *
        FROM
            BOOK
        WHERE
            book_id IN {str(reco).replace('[','(').replace(']',')')}
    """

    if connection.is_connected():
        df = pd.read_sql(query, connection)
        connection.close()

    return df.to_dict('records')