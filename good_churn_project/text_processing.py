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

def train_text_clusters(db, db_user, train_user_ids, no_clusters):
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''SELECT user_id
    string_agg(body,' ') AS answer,
    count(user_id) AS answer_count
    FROM answers
    GROUP BY user_id;
    '''
    cur.execute(sql)
    documents = cur.fetchall()
    conn.close()
    answer_df = pd.DataFrame(columns= ['user_id', 'answer_text', 'answers_count'])
    for doc in documents:
        if doc[0] in train_user_ids:
            ans_series = pd.Series(list(doc), index = ['user_id', 'answer_text', 'answers_count'])
            answer_df = answer_df.append(ans_series, ignore_index=True)
    vectorizer = TfidfVectorizer(stop_words='english')
    model = vectorizer.fit_transform(answer_df['answer_text'])
    km = KMeans(n_clusters=no_clusters, init='k-means++', max_iter=100, n_init=10, n_jobs=-1, random_state=49)
    answer_df['answers_clusters'] = km.fit(model).labels_
    just_dummies = pd.get_dummies(answer_df['answers_clusters'], prefix='text')
    answer_df = pd.concat([answer_df.user_id, just_dummies], axis=1)
    return answer_df, vectorizer, km

def train_text_clusters2(db, db_user, train_user_df, no_clusters):
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
    km = KMeans(n_clusters=no_clusters, init='k-means++', max_iter=100, n_init=10, n_jobs=-1, random_state=49)
    response_df['response_clusters'] = km.fit(model).labels_
    just_dummies = pd.get_dummies(response_df['response_clusters'], prefix='text')
    response_df = pd.concat([response_df.user_id, response_df.response_len, just_dummies], axis=1)
    return response_df, vectorizer, km

def train_text_clusters3(db, db_user, train_user_df, no_SVs):
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

def test_text_clusters(db, db_user, test_user_ids, vectorizer_model, km_model):
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''SELECT user_id,
    string_agg(body,' ') AS answer,
    count(user_id) AS answer_count
    FROM answers
    GROUP BY user_id;
    '''
    cur.execute(sql)
    documents = cur.fetchall()
    conn.close()
    answer_df = pd.DataFrame(columns= ['user_id', 'answer_text', 'answers_count'])
    for doc in documents:
        if doc[0] in test_user_ids:
            ans_series = pd.Series(list(doc), index = ['user_id', 'answer_text', 'answers_count'])
            answer_df = answer_df.append(ans_series, ignore_index=True)
    test_data_vect = vectorizer_model.transform(answer_df['answer_text'])
    answer_df['answers_clusters'] = km_model.predict(test_data_vect)
    just_dummies = pd.get_dummies(answer_df['answers_clusters'], prefix='text')
    answer_df = pd.concat([answer_df.user_id, just_dummies], axis=1)
    return answer_df

def test_text_clusters2(db, db_user, test_user_df, vectorizer_model, km_model):
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
            resp_series = pd.Series([test_user.user_id, '0', 0], index = ['user_id', 'response_text', 'response_count'])
            resp_series['response_len'] = 0
            response_df = response_df.append(resp_series, ignore_index=True)
            continue
        resp_series = pd.Series(query[0], index = ['user_id', 'response_text', 'response_count'])
        resp_series['response_len'] = len(resp_series.response_text)
        response_df = response_df.append(resp_series, ignore_index=True)
    conn.close()

    test_data_vect = vectorizer_model.transform(response_df['response_text'])
    response_df['response_clusters'] = km_model.predict(test_data_vect)
    f_list = range(km_model.n_clusters)
    just_dummies = pd.get_dummies(list(response_df['response_clusters'].values) + f_list, prefix='text')
    neg_feature = -km_model.n_clusters
    just_dummies = just_dummies[:neg_feature]
    response_df = pd.concat([response_df.user_id, response_df.response_len, just_dummies], axis=1)
    return response_df

def test_text_clusters3(db, db_user, test_user_df, vectorizer_model, svd):
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
            resp_series = pd.Series([test_user.user_id, '0', 0], index = ['user_id', 'response_text', 'response_count'])
            resp_series['response_len'] = 0
            response_df = response_df.append(resp_series, ignore_index=True)
            continue
        resp_series = pd.Series(query[0], index = ['user_id', 'response_text', 'response_count'])
        resp_series['response_len'] = len(resp_series.response_text)
        response_df = response_df.append(resp_series, ignore_index=True)
    conn.close()

    test_data_vect = vectorizer_model.transform(response_df['response_text'])

    svdMatrix = svd.transform(test_data_vect)
    response_df = pd.concat([response_df.user_id, response_df.response_len, pd.DataFrame(svdMatrix)], axis=1)
    return response_df


def add_class_to_df(df, good_churn_user_id):
    '''
    Appends a class column to a DataFrame that contains a user_id column, given
    a list of user_id's for the 1 class
    '''
    c = np.zeros(len(df.user_id))
    for index, user_id in enumerate(df.user_id):
        if user_id in good_churn_user_id:
            c[index] = 1
    df['class'] = c
    return df
