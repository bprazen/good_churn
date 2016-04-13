import psycopg2 as pg2
import pandas as pd
from sqlalchemy import create_engine

# note: need to determine how to deal with tables that have user_id insted of sender_id
def user_df(db, db_user, table):
    '''Create a Pandas DataFrame containing the user_id, first activity (start_date)
    latest activity (stop_date) through a db querry

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: the database table to querry

    Returns
    -------

    Example
    -------
    '''
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql ='''
    SELECT sender_id, MIN(created_at) AS start_date, MAX(created_at) AS stop_date, count(created_at) AS activity_count
    FROM {}
    GROUP BY sender_id
    ;
    '''.format(table)
    cur.execute(sql)
    user_table = pd.DataFrame(cur.fetchall(), columns= ['user_id', 'start_date', 'stop_date', 'activity_count'])
    conn.close()
    return user_table

def activity_dates_df(db, db_user, table, app_user):
    # Create a Pandas DataFrame containing the activity date, next ativity date aand time before next activity
    #for a given app user.
    #
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = 'SELECT created_at FROM {} WHERE sender_id = {};'.format(table, int(app_user))
    cur.execute(sql)
    activity_dates = pd.DataFrame(cur.fetchall(), columns= ['date']).sort(['date'], ascending=True)
    next_activity = activity_dates['date'][1:]
    activity_dates = activity_dates[:len(activity_dates)-1]
    next_activity.index = activity_dates.index
    activity_dates['next'] = next_activity
    activity_dates['dif_time'] = activity_dates['next']-activity_dates['date']
    conn.close()
    return activity_dates

def user_stats_df(db, db_user, table):
    # Create a Pandas DataFrame containing the activity statistics for a list of users.
    #
    user_stats = pd.DataFrame(columns= ['app_user','first_use', 'last_use', 'time_with_app', 'num_uses',
                                         'min_away', 'max_away', 'avg_away', 'median_away'])
    user_table = user_df(db, db_user, table)
    for user_id in user_table['user_id']:
        # Addresses data that does not contain a user_id
        if not user_id > 0:
            continue
        activity_dates = activity_dates_df(db, db_user, table, user_id)
        #Addresses users that only have one activity.
        if len(activity_dates) == 0:
            user_stats = user_stats.append( pd.Series([user_id, pd.tslib.Timedelta(0),
                                                       pd.tslib.Timedelta(0),
                                                       pd.tslib.Timedelta(0),
                                                       1,
                                                       pd.tslib.Timedelta(0),
                                                       pd.tslib.Timedelta(0),
                                                       pd.tslib.Timedelta(0),
                                                       pd.tslib.Timedelta(0)],
                                                      index=['app_user','first_use', 'last_use', 'time_with_app', 'num_uses',
                                                             'min_away', 'max_away', 'avg_away', 'median_away']),
                                           ignore_index=True)
        else:
            user_stats = user_stats.append( pd.Series([user_id, activity_dates['date'].iloc[0],
                                                       activity_dates['next'].iloc[-1],
                                                       activity_dates['next'].iloc[-1]-activity_dates['date'].iloc[0],
                                                       len(activity_dates['date'])+1,
                                                       min(activity_dates['dif_time']),
                                                       max(activity_dates['dif_time']),
                                                       activity_dates['dif_time'].sum()/len(activity_dates['dif_time']),
                                                       activity_dates['dif_time'].median()],
                                                      index=['app_user','first_use', 'last_use', 'time_with_app', 'num_uses',
                                                             'min_away', 'max_away', 'avg_away', 'median_away']), ignore_index=True)

        return user_stats

def create_activity_table(db, db_user):
    # Create a PostgreSQL table containing user activity from multiple tables
    # This wil be used to classify users as active and inactive during different periods
    #
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE activity AS
    SELECT sender_id AS user_id, created_at AS date, id, 'notifications' AS type
    FROM notifications
    ''')
    cur.execute('''INSERT INTO activity
    SELECT user_id, created_at AS date, id, 'answer_likes' AS type
    FROM answer_likes
    ''')
    cur.execute('''INSERT INTO activity
    SELECT user_id, created_at AS date, id, 'answers' AS type
    FROM answers
    ''')
    cur.execute('''INSERT INTO activity
    SELECT from_user_id AS user_id, created_at AS date, id, 'connections' AS type
    FROM connections
    ''')
    conn.commit()
    cur.execute('''SELECT count(*)
    FROM activity
    ''')
    table_size = cur.fetchall()
    conn.close()
    return table_size


def count_active(db, db_user, table, last_date):
    # Count the users that are active beyond a date. Last date formated like '2016-03-01'
    #
    #
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''WITH    churn AS
    (
    SELECT (MAX(date) > '{}') AS still_in
    FROM activity
    GROUP BY user_id
    )
    SELECT COUNT (still_in)
    FROM churn
    WHERE still_in = 't'
    ;
    '''.format(last_date)
    cur.execute(sql)
    count = cur.fetchall()
    conn.close()
    return count

def record_table(db, db_user, table):
    # Save tables that are timeconsuming to creat in postgreSQL DB
    engine_str = 'postgresql://{}@localhost:5432/{}'.format(db_user, db)
    engine = create_engine(engine_str)
    table.to_sql(table, engine)
    return

def activity_dates_range_df(db, db_user, table, app_user, date1, date2):
    # Create a Pandas DataFrame containing the activity date and id
    # for a given app user over a period between date1 and date2.
    #
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = "SELECT date, id FROM {} WHERE user_id = {} AND date > '{}' AND date < '{}';".format(table, int(app_user), date1, date2)
    cur.execute(sql)
    activity_dates = pd.DataFrame(cur.fetchall(), columns= ['date', 'id']).sort(['date'], ascending=True)
    conn.close()
    activity_dates = activity_dates.reset_index(drop=True)
    return activity_dates

def idendify_good_churn_user(db, db_user, table, app_user, leave_time, prechurn_time, prechurn_act, postchurn_time, postchurn_act):
    # Create a Pandas DataFrame containing the time regions that qualify
    # as good churn for a user given the leave_time (days), prechurn_time (days), prechurn activity
    # postchurn_time (days) and postchurn activity.
    #
    act_one_user = activity_dates_df(db, db_user, table, app_user)
    activity_on_leaving = act_one_user[act_one_user.dif_time > pd.tslib.Timedelta(days=leave_time)]
    activity_on_leaving = activity_on_leaving.reset_index(drop=True)
    good_churns = pd.DataFrame(columns= ['user_id', 'churn_date', 'No_prechurn_activities', 'No_postchurn_activities'])
    for index, row in activity_on_leaving.iterrows():
        date1 = str(row.date.date()-pd.tslib.Timedelta(days=14))
        date2 = str(row.date.date())
        pre_leave_df = activity_dates_range_df(db, db_user, table, app_user, date1, date2)
        date1 = str(row.next.date())
        date2 = str(row.next.date()+pd.tslib.Timedelta(days=postchurn_time))
        post_leave_df = activity_dates_range_df(db, db_user, table, app_user, date1, date2)
        if len(pre_leave_df) > prechurn_act and len(post_leave_df) > postchurn_act:
            series = pd.Series([app_user, row.date.date(), len(pre_leave_df), len(post_leave_df)], index=['user_id', 'churn_date', 'No_prechurn_activities', 'No_postchurn_activities'])
            good_churns = good_churns.append(series, ignore_index=True)
    return good_churns

    def idendify_good_churn_across_user(db, db_user, table, user_ids, leave_time, prechurn_time, prechurn_act, postchurn_time, postchurn_act):
    # Create a Pandas DataFrame containing the time regions that qualify
    # as good churn for a list of users given the user_ids, leave_time (days), prechurn_time (days), prechurn activity
    # postchurn_time (days) and postchurn activity.
    #
    good_churns = pd.DataFrame(columns= ['user_id', 'churn_date', 'No_prechurn_activities', 'No_postchurn_activities'])
    for user_id in user_ids:
        churn = idendify_good_churn_user(db, db_user, table, user_id, leave_time, prechurn_time, prechurn_act, postchurn_time, postchurn_act)
        good_churns = good_churns.append(churn, ignore_index=True)
    return good_churns

def idendify_bad_churn_users(db, db_user, table, time_gone, prechurn_act):
    # Create a Pandas DataFrame containing the time users that qualify
    # as bad churn given the user_ids, time_gone (days) and  prechurn activity.
    #
    date_of_leave = pd.tslib.Timestamp('2016-04-04')-pd.tslib.Timedelta(days=time_gone)
    date_of_leave = str(date_of_leave.date())
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''WITH churn AS
    (
    SELECT user_id, MAX(date) AS last_day, count(date) AS activity_count, (MAX(date) > '{}') AS still_in
    FROM activity
    GROUP BY user_id
    )
    SELECT user_id, last_day, activity_count
    FROM churn
    WHERE still_in = 'f'
    and activity_count > {}
    ;
    '''.format(date_of_leave, prechurn_act)
    cur.execute(sql)
    bad_churn = pd.DataFrame(cur.fetchall(), columns= ['user_id', 'last_day', 'activity_count'])
    conn.close()
    return bad_churn


def idendify_test_users(db, db_user, table, time_gone_low, time_gone_high, prechurn_act):
    # Create a Pandas DataFrame containing users that will serve as test data
    # given a time window of interest and prechurn activity.
    #
    date__gone_high = pd.tslib.Timestamp('2016-04-04')-pd.tslib.Timedelta(days=time_gone_low)
    date__gone_low = pd.tslib.Timestamp('2016-04-04')-pd.tslib.Timedelta(days=time_gone_high)
    date__gone_high = str(date__gone_high.date())
    date__gone_low = str(date__gone_low.date())

    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''WITH test AS
    (
    SELECT user_id, MAX(date) AS last_day, count(date) AS activity_count, (MAX(date) > '{}' AND MAX(date) < '{}') AS in_window
    FROM activity
    GROUP BY user_id
    )
    SELECT user_id, last_day, activity_count
    FROM test
    WHERE in_window = 't'
    and activity_count > {}
    ;
    '''.format(date__gone_low, date__gone_high, prechurn_act)
    cur.execute(sql)
    test_data = pd.DataFrame(cur.fetchall(), columns= ['user_id', 'last_day', 'activity_count'])
    conn.close()
    return test_data
