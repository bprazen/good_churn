import psycopg2 as pg2
import pandas as pd


def feature_maker(db, db_user, user_id, date1, date2):
    # construct a feature set for a user in time range
    #
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
    int(send_notification[0][0]), int(median_away[0][0]), age]
