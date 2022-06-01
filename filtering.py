import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np
from scipy.spatial import distance as sp_dist

"""
TAGS = ['fiction', 'fantasy', 'romance', 'classic', 'mystery', 'kindle', 'sci-fi', 'literature',
        'horror', 'contemporary', 'adventure', 'historical', 'adult', 'paranormal',
        'thriller', 'history', 'dystopia', 'audio', 'children', 'school', 'philosophy', 'novel', 'young'
        ]
"""

pd.options.mode.chained_assignment = None


# Return a user favorite tags
def getFavoriteTags(user_mail, connection):
    query = f"""
        SELECT
            first_fav_category
            ,second_fav_category
            ,third_fav_category
        FROM
            USER
        WHERE
            mail = '{user_mail}'
    """

    if connection.is_connected():
        df = pd.read_sql(query, connection)

    return df.loc[0].tolist()


# Return the most popular books according to the user preferences
def getPopularReco(user_mail, connection):

    favorite_tags = getFavoriteTags(user_mail, connection)

    query = f"""
        SELECT
            book_id
        FROM
            BOOK B INNER JOIN TAGGED TG
                ON B.book_id = TG.book_id
            INNER JOIN TAG TA
                ON TG.tag_id = TA.tag_id
        ORDER BY
            
        LIMIT 5 
    """

    if connection.is_connected():
        df = pd.read_sql(query, connection)

# Return the number of reviews of a given user


def getNumberReview(user_mail, connection):
    query = f"""
        SELECT
            COUNT(*) AS NB
        FROM
            USER U INNER JOIN REVIEW R
                ON U.user_id = R.user_id
        WHERE
            U.mail = '{user_mail}'
    """

    if connection.is_connected():
        df = pd.read_sql(query, connection)

    return df['NB'][0]


# Encodes processed_data that need to be encoded according one book
def encodeLabels(df, book_id, columns):
    for col in columns:
        df[col] = (df[col] == df.loc[book_id, col]).astype(float)
    return df


# Get similarity rates
def getSim(array1, array2, method='cos'):
    if method == 'cos':
        return sp_dist.cosine(array1, array2)
    if method == 'euc':
        return sp_dist.euclidean(array1, array2)
    if method == 'pea':
        num = sum([x1*x2 for x1, x2 in zip(array1, array2)])
        denom1 = 0
        denom2 = 0
        for x1, x2 in zip(array1, array2):
            if x1 != 0 and x2 != 0:
                denom1 += x1*x1
                denom2 += x2*x2
        denom = np.sqrt(denom1)*np.sqrt(denom2)
        if denom == 0:
            return 0
        else:
            return num/denom


def getSimUsers(initial_user, users, method='cos'):
    sim = []
    for user in users:
        score = getSim(initial_user[1], user[1], method)
        sim.append((user[0], score))
    sim.sort(key=lambda x: x[1], reverse=True)
    return sim


class Filtering:

    def __init__(self, filter_base=None):
        self.filter_base = filter_base
        self.data = None
        self.process = None
        self.connection = None

    def setFilterBase(self, filter_base):
        self.filter_base = filter_base

    def setConnection(self, connection):
        self.connection = connection


class ContentBasedFiltering(Filtering):

    def __init__(self, filter_base=None):
        super().__init__(filter_base=filter_base)

    def loadData(self, top=10000):
        query = f"""
            SELECT
                B.*
                ,W.author_id
            FROM
                BOOK B INNER JOIN WROTE W
                    ON B.book_id = W.book_id
            WHERE
                B.book_id = {self.filter_base}
            UNION
            SELECT
                B.*
                ,W.author_id
            FROM
                BOOK B INNER JOIN WROTE W
                    ON B.book_id = W.book_id
            LIMIT {top}
        """
        if self.connection.is_connected():
            df = pd.read_sql(query, self.connection).drop_duplicates(
                subset=['book_id'], keep='first')

        df.set_index('book_id', inplace=True, drop=True)
        self.data = df

    def processData(self):
        to_keep = ['author_id', 'publisher', 'publication_year', 'format']
        self.process = self.data[to_keep].copy()

        # process year column
        self.process['publication_year'].fillna(0, inplace=True)
        self.process['publication_year'][self.process['publication_year'] < 1960] = 1960
        self.process['publication_year'] = ((self.process['publication_year'] - self.process['publication_year'].min())
                                            / (self.process['publication_year'].max()-self.process['publication_year'].min()))

        # process format column
        top_format = ['Paperback', 'Hardcover', 'ebook',
                      'Kindle Edition', 'Mass Market Paperback', 'Audiobook']
        self.process['format'].fillna('Undefined', inplace=True)
        self.process['format'].replace('ebook', 'Ebook', inplace=True)
        self.process['format'] = self.process['format'].apply(
            lambda x: 'Audiobook' if 'Aud' in x else x)
        self.process['format'] = self.process['format'].apply(
            lambda x: 'Other' if x not in top_format and x != 'Undefined' else x)

        # process label columns
        to_encode = ['author_id', 'publisher', 'format']
        self.process = encodeLabels(self.process, self.filter_base, to_encode)

    def getBestRecommendations(self, book_id, data, top=10, method='cos'):

        reco = {}

        refer = data.loc[book_id].tolist()

        book_ids = data.index.tolist()
        items = data.to_numpy()

        for index, item in zip(book_ids, items):
            score = getSim(refer, item, method)
            if index != book_id:
                reco[index] = score

        reco = sorted(reco.items(), key=lambda kv: kv[1])

        return [book_id for book_id, _ in reco[:top]]

    def filter(self, top=10):

        query = f"""
            SELECT
                TGD.book_id
                ,TAG.name
                ,TGD.count / SUM(TGD.count) OVER(PARTITION BY TGD.book_id) AS "perc"
            FROM
                TAGGED TGD INNER JOIN TAG TAG
                    ON TGD.tag_id = TAG.tag_id
            WHERE
                TGD.book_id IN {str(self.process.index.tolist()).replace('[','(').replace(']',')')}
        """

        if self.connection.is_connected():
            tag = pd.read_sql(query, self.connection).pivot_table(
                index='book_id', columns='name', values='perc').fillna(0)

        final_data = self.process.merge(
            tag, how='inner', left_index=True, right_index=True)

        return self.getBestRecommendations(self.filter_base, final_data, top=top, method='cos')


class CollabFiltering(Filtering):

    def __init__(self, filter_base=None):
        super().__init__(filter_base=filter_base)

    def loadData(self, top=50):
        query1 = f"""
            SELECT
                user_id
            FROM
                USER
            WHERE
                mail = '{self.filter_base}'
        """

        if self.connection.is_connected():
            user_id_df = pd.read_sql(query1, self.connection)

        self.filter_base = user_id_df['user_id'][0]

        print(self.filter_base)

        query2=f"""
            SELECT
                U.user_id
                ,(SELECT
                    COUNT(*)
                FROM
                    (SELECT
                        R1.book_id
                    FROM
                        REVIEW R1
                    WHERE
                        R1.user_id = {self.filter_base}
                    AND
                        R1.book_id IN (SELECT
                                        R2.book_id
                                    FROM
                                        REVIEW R2
                                    WHERE
                                        R2.user_id = U.user_id
                                    )
                    ) D
                ) AS 'NB'
            FROM
                USER U
            HAVING
                NB > 0
            ORDER BY
                2 DESC
            LIMIT {top+1}
        """

        if self.connection.is_connected():
            same_read_df = pd.read_sql(query2, self.connection)

        print(same_read_df)

        query3=f"""
            SELECT
                user_id
                ,book_id
                ,rating
            FROM
                REVIEW
            WHERE
                user_id = {self.filter_base}
            UNION
            SELECT
                user_id
                ,book_id
                ,rating
            FROM
                REVIEW
            WHERE
                user_id IN {str(same_read_df['user_id'].tolist()).replace('[','(').replace(']',')')}
        """

        if self.connection.is_connected():
            df = pd.read_sql(query3, self.connection)

        print(df)

        self.data = df

    def processData(self):
        means = self.data.groupby(['user_id'], as_index=False, sort=False).mean().rename(columns={'rating': 'mean_rating'})
        means.drop(columns=['book_id'], inplace=True)
        self.data = self.data.merge(means, on='user_id', how='left', sort=False)
        self.data['adjusted_rating'] = self.data['rating'] - self.data['mean_rating']
        self.process = self.data.pivot_table(index='user_id', columns='book_id', values='adjusted_rating').fillna(0)

    def getBestRecommendations(self, df, top=10):
        reco = []
        sumsim = df['sim'].sum()
        for book_id in df.columns[:-1]:
            df[book_id] = df[book_id]*df['sim']
            reco.append((book_id, sum(df[book_id].tolist())/sumsim))
        reco.sort(key=lambda x: x[1], reverse=True)
        return reco[:top]

    def filter(self, top=10):
        print(self.process)
        initial_user = (self.filter_base, self.process.loc[self.filter_base])
        users = [(index, value) for index, value in zip(
            self.process.index, self.process.values) if index != self.filter_base]
        sim_users = getSimUsers(initial_user, users, method='pea')

        self.process = self.process.loc[[user for user, _ in sim_users], :]
        self.process['sim'] = [sim for _, sim in sim_users]
        print(self.process)

        reco = self.getBestRecommendations(self.process, top=top)
        print(reco)

        return [book_id for book_id, _ in reco]


if __name__ == '__main__':
    connection = mysql.connector.connect(
        host='ds50-mysql-do-user-9644544-0.b.db.ondigitalocean.com',
        database='ds50',
        user='ds50',
        password='AVNS_4ybSd0CoPKnCL5F',
        port='25060'
    )

    # test mail : rjohnson@gmail.com
    #print(getNumberReview('aadams18@hotmail.com', connection))
    #print(getFavoriteTags('aadams18@hotmail.com', connection))

    '''
    filtering = ContentBasedFiltering()
    filtering.setFilterBase(filter_base=27421523)
    filtering.setConnection(connection)
    filtering.loadData()
    filtering.processData()
    print(filtering.filter())
    '''

    filtering = CollabFiltering()
    filtering.setFilterBase(filter_base='aadams18@hotmail.com')
    filtering.setConnection(connection)
    filtering.loadData()
    filtering.processData()
    print(filtering.filter())
