from filtering import ContentBasedFiltering, CollabFiltering, getNumberReview, getPopularReco
from request_data import fillDataframeNulls

import mysql.connector
from mysql.connector import Error
import pandas as pd


def getConnectionFromServer():
    return mysql.connector.connect(
        host='ds50-mysql-do-user-9644544-0.b.db.ondigitalocean.com',
        database='ds50',
        user='ds50',
        password='AVNS_4ybSd0CoPKnCL5F',
        port='25060')


def getReco(method='content', filter_base=None):
    connection = getConnectionFromServer()

    if method == 'content' and filter_base:
        filter_base = int(filter_base)
        filtering = ContentBasedFiltering()
        filtering.setFilterBase(filter_base)
        filtering.setConnection(connection)
        filtering.loadData()
        filtering.processData()
        reco = filtering.filter()

    if method == 'collab' and filter_base:
        #MINIMUM_RECQUIRED = 5
        #nb_review = getNumberReview(filter_base, connection)

        # if nb_review < MINIMUM_RECQUIRED:
        if True:
            filtering = CollabFiltering()
            filtering.setFilterBase(filter_base=filter_base)
            filtering.setConnection(connection)
            filtering.loadData()
            filtering.processData()
            reco = filtering.filter()
        else:
            reco = getPopularReco(filter_base, connection)

    query = f"""
        SELECT
            *
        FROM
            BOOK
        WHERE
            book_id IN {str(reco).replace('[','(').replace(']',')')}
    """

    df = pd.read_sql(query, connection)
    df = fillDataframeNulls(df)
    return df.to_dict('records')
