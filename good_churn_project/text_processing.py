import psycopg2 as pg2
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
stop_words = stopwords.words('english')
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
from sklearn.decomposition import TruncatedSVD


def train_text_clusters(db, db_user, train_user_df, no_SVs):
    '''
    transforms text data from traing users using TF-IDF and reduces features
    using SVD.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    train_user_ids: string containing user_ids to train the text anaysis model
    no_SVs: int containing the number of singular values to retain.

    Returns
    -------
    answer_df: pandas DataFrame
    vectorizer: model from TF-IDF
    svd: model from singular value decomposition

    Example
    -------

    '''
    response_df = pd.DataFrame(columns= ['user_id', 'response_text', 'response_count'])
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    for index, train_user in train_user_df.iterrows():
        sql = '''
        SELECT user_id,
        string_agg(body,' ') AS response_text,
        count(user_id) AS response_count
        FROM answers
        WHERE user_id = {}
        AND created_at > '{}'
        AND created_at < '{}'
        GROUP BY user_id
        LIMIT 10;
        '''.format(train_user.user_id, train_user.pre_churn_date, train_user.churn_date)
        cur.execute(sql)
        query = cur.fetchall()
        if not len(query) > 0:
            resp_series = pd.Series([train_user.user_id, '0', 0], index = ['user_id', 'response_text', 'response_count'])
            resp_series['response_len'] = 0
            response_df = response_df.append(resp_series, ignore_index=True)
            continue
        resp_series = pd.Series(query[0], index = ['user_id', 'response_text', 'response_count'])
        resp_series['response_len'] = len(resp_series.response_text)
        response_df = response_df.append(resp_series, ignore_index=True)
    conn.close()
    vectorizer = TfidfVectorizer(stop_words='english')
    model = vectorizer.fit_transform(response_df['response_text'])
    svd = TruncatedSVD(n_components = no_SVs)
    svdMatrix = svd.fit_transform(model)
    response_df = pd.concat([response_df.user_id, response_df.response_len, pd.DataFrame(svdMatrix)], axis=1)
    return response_df, vectorizer, svd


def test_text_clusters(db, db_user, test_user_df, vectorizer_model, svd):
    '''
    transforms text data from test users using TF-IDF and SVD models built
    with training data.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    test_user_df: DataFrame containing user_ids
    vectorizer_model: TF-IDF model built with training data.
    svd: SVD model built with training data.

    Returns
    -------
    response_df: pandas DataFrame

    Example
    -------

    '''

    response_df = pd.DataFrame(columns= ['user_id', 'response_text', 'response_count'])
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    for index, test_user in test_user_df.iterrows():
        sql = '''
        SELECT user_id,
        string_agg(body,' ') AS response_text,
        count(user_id) AS response_count
        FROM answers
        WHERE user_id = {}
        AND created_at > '{}'
        AND created_at < '{}'
        GROUP BY user_id
        LIMIT 10;
        '''.format(test_user.user_id, test_user.pre_churn_date, test_user.churn_date)
        cur.execute(sql)
        query = cur.fetchall()
        if not len(query) > 0:
            resp_series = pd.Series([test_user.user_id, '0', 0],
                                    index = ['user_id', 'response_text',
                                                'response_count'])
            resp_series['response_len'] = 0
            response_df = response_df.append(resp_series, ignore_index=True)
            continue
        resp_series = pd.Series(query[0], index = ['user_id', 'response_text',
                                                    'response_count'])
        resp_series['response_len'] = len(resp_series.response_text)
        response_df = response_df.append(resp_series, ignore_index=True)
    conn.close()

    test_data_vect = vectorizer_model.transform(response_df['response_text'])

    svdMatrix = svd.transform(test_data_vect)
    response_df = pd.concat([response_df.user_id, response_df.response_len,
                                pd.DataFrame(svdMatrix)], axis=1)
    return response_df
