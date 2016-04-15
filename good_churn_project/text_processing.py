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

def train_text_clusters(db, db_user, train_user_ids, no_clusters):
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
    #answer_df = pd.concat([answer_df, just_dummies], axis=1)
    answer_df = pd.concat([answer_df.user_id, just_dummies], axis=1)
    return answer_df

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
