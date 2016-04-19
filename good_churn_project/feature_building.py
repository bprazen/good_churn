import psycopg2 as pg2
import pandas as pd


def feature_maker(db, db_user, user_id, date1, date2):
    ''' construct a feature set for a user in time range
    Calls on following tables: answer_likes, answers, connections, notifications
    user_stats and users.
    '''
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''SELECT count(answer_id)
    FROM answer_likes
    WHERE user_id = {}
    AND created_at < '{}'
    AND created_at > '{}'
    ;
    '''.format(user_id, date2, date1)
    cur.execute(sql)
    answer_likes = cur.fetchall()

    sql = '''SELECT count( id )
    FROM answers
    WHERE user_id = {}
    AND created_at < '{}'
    AND created_at > '{}'
    ;
    '''.format(user_id, date2, date1)
    cur.execute(sql)
    answers = cur.fetchall()

    sql = '''SELECT count( id )
    FROM connections
    WHERE to_user_id = {}
    AND created_at < '{}'
    AND created_at > '{}'
    ;
    '''.format(user_id, date2, date1)
    cur.execute(sql)
    accepted_connections = cur.fetchall()

    sql = '''SELECT count( id )
    FROM connections
    WHERE from_user_id = {}
    AND created_at < '{}'
    AND created_at > '{}'
    ;
    '''.format(user_id, date2, date1)
    cur.execute(sql)
    made_connections = cur.fetchall()

    sql = '''SELECT count( id )
    FROM notifications
    WHERE sender_id = {}
    AND created_at < '{}'
    AND created_at > '{}'
    ;
    '''.format(user_id, date2, date1)
    cur.execute(sql)
    send_notification = cur.fetchall()

    sql = '''SELECT median_away
    FROM user_stats
    WHERE app_user = {}
    ;
    '''.format(user_id)
    cur.execute(sql)
    median_away = cur.fetchall()

    sql = '''SELECT avg_away
    FROM user_stats
    WHERE app_user = {}
    ;
    '''.format(user_id)
    cur.execute(sql)
    avg_away = cur.fetchall()

    #sql = '''SELECT first_use
    #FROM user_stats
    #WHERE app_user = {}
    #;
    #'''.format(user_id)
    #cur.execute(sql)
    #time_w_app = date2 - cur.fetchall()[0][0]
    #time_w_app = time_w_app.days

    sql = '''SELECT birthdate
    FROM users
    WHERE id = {}
    ;
    '''.format(user_id)
    cur.execute(sql)
    bd = cur.fetchall()
    age = (pd.tslib.Timestamp('2016-04-04')-bd[0][0])
    age = age.days/365.0

    conn.close()
    return [user_id, int(answer_likes[0][0]), int(answers[0][0]),
    int(accepted_connections[0][0]), int(made_connections[0][0]),
    int(send_notification[0][0]), int(median_away[0][0]), int(avg_away[0][0]), age]

def feature_df_maker(db, db_user, user_list, date1, date2):
    '''
    Constructs feature DataFrame for list of users.
    '''
    feature_df = pd.DataFrame(columns= ['user_id', 'answer_likes', 'answers',
    'accepted_connections', 'made_connections',
    'send_notification', 'median_away', 'avg_away','age'])
    for user in user_list:
        features = pd.Series(feature_maker(db, db_user, user, date1, date2), index= ['user_id', 'answer_likes', 'answers',
         'accepted_connections', 'made_connections','send_notification', 'avg_away', 'age'])
        feature_df = feature_df.append(features, ignore_index=True)
    return feature_df

def feature_df_maker2(db, db_user, user_df):
    '''
    Constructs feature DataFrame for list of users.
    '''
    feature_df = pd.DataFrame(columns= ['user_id', 'response_likes', 'no_responses',
    'accepted_connections', 'made_connections',
    'send_notification', 'median_away', 'avg_away', 'age'])
    for index, user in user_df.iterrows():
        features = pd.Series(feature_maker(db, db_user, user.user_id, user.pre_churn_date, user.churn_date), index= ['user_id', 'response_likes', 'no_responses',
         'accepted_connections', 'made_connections','send_notification', 'median_away', 'avg_away', 'age'])
        feature_df = feature_df.append(features, ignore_index=True)
    return feature_df
