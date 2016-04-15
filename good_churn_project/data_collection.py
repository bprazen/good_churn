import psycopg2 as pg2
import pandas as pd
from sqlalchemy import create_engine

# note: need to determine how to deal with tables that have user_id insted of sender_id
def user_df_maker(db, db_user, table):
    '''Create a Pandas DataFrame containing the user_id, first activity (start_date)
    latest activity (stop_date) through a db querry

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry

    Returns
    -------
    Pandas DataFrame

    Example
    -------
    >>>users_df = user_df_maker(db, db_user, 'activity')
    >>>users_df.user_id[1:3].values
    [ 251.,  2848.]
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
    user_df = pd.DataFrame(cur.fetchall(), columns= ['user_id', 'start_date', 'stop_date', 'activity_count'])
    conn.close()
    return user_df

def activity_dates_df(db, db_user, table, app_user):
    ''' Create a Pandas DataFrame that shows the time before the
    next activity for a given user.
    DataFrame contains the activity date, next ativity date and time before next activity
    for a given app user.


    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry
    app_user: int user_id

    Returns
    -------

    Example
    -------
    '''
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = 'SELECT date FROM {} WHERE user_id = {};'.format(table, int(app_user))
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

def import_user_stats(db, db_user):
    ''' Pull user_stats table from database.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database

    Returns
    -------
    DataFrame containing data from user_stats table which was added to the the database previously.

    Example
    -------
    '''
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql ='''SELECT app_user, first_use, last_use, time_with_app, num_uses, min_away, max_away, avg_away, median_away
    FROM user_stats;
    '''
    cur.execute(sql)
    user_stats = pd.DataFrame(cur.fetchall(), columns= ['app_user', 'first_use', 'last_use', 'time_with_app', 'num_uses', 'min_away', 'max_away', 'avg_away', 'median_away'])
    conn.close()
    user_stats.time_with_app = pd.to_timedelta(user_stats.time_with_app, unit='ns')
    user_stats.min_away = pd.to_timedelta(user_stats.min_away, unit='ns')
    user_stats.max_away = pd.to_timedelta(user_stats.max_away, unit='ns')
    user_stats.avg_away = pd.to_timedelta(user_stats.avg_away, unit='ns')
    user_stats.median_away = pd.to_timedelta(user_stats.median_away, unit='ns')
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
    act_one_user = activity_dates_df(db, db_user, table, app_user) # activity for each user
    activity_on_leaving = act_one_user[act_one_user.dif_time > pd.tslib.Timedelta(days=leave_time)] # activities that are followed by exess leave
    activity_on_leaving = activity_on_leaving.reset_index(drop=True)
    good_churns = pd.DataFrame(columns= ['user_id', 'churn_date', 'No_prechurn_activities', 'No_postchurn_activities'])
    for index, row in activity_on_leaving.iterrows(): # for each activity that is before excess leave, churn
        date1 = str(row.date.date()-pd.tslib.Timedelta(days=prechurn_time))
        date2 = str(row.date.date())
        pre_leave_df = activity_dates_range_df(db, db_user, table, app_user, date1, date2) # activity in the time range before the churn
        date1 = str(row.next.date())
        date2 = str(row.next.date()+pd.tslib.Timedelta(days=postchurn_time))
        post_leave_df = activity_dates_range_df(db, db_user, table, app_user, date1, date2) # activity after the churn
        if len(pre_leave_df) > prechurn_act and len(post_leave_df) > postchurn_act: # only keep values with enough activity
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
