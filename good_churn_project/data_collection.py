import psycopg2 as pg2
import pandas as pd
from sqlalchemy import create_engine


def user_df_maker(db, db_user, table):
    '''Create a Pandas DataFrame containing the user_id, first activity
    (start_date) latest activity (stop_date) through a db query

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
    >>> users_df = data_collection.user_df_maker(db, db_user, 'answers')
    >>> users_df.user_id[1:3].values
    array([2848, 3565])
    '''
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    column_qurey = '''
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='{}'
    ;
    '''.format(table)
    cur.execute(column_qurey)
    columns = cur.fetchall()
    columns = [c[0] for c in columns]
    if 'created_at' in columns:
        created = 'created_at'
    else:
        created = 'date'

    sql = '''
    SELECT user_id, MIN({}) AS start_date,
    MAX({}) AS stop_date,
    count({}) AS activity_count
    FROM {}
    GROUP BY user_id
    ;
    '''.format(created, created, created, table)
    cur.execute(sql)
    user_df = pd.DataFrame(cur.fetchall(), columns=['user_id', 'start_date',
                                                    'stop_date',
                                                    'activity_count'])
    conn.close()
    return user_df


def activity_dates_df(db, db_user, table, app_user):
    ''' Create a Pandas DataFrame that shows the time before the
    next activity for a given user.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry
    app_user: int user_id

    Returns
    -------
    DataFrame containing the activity date, next ativity date and time before
    next activity for a given app user.

    Example
    -------
    >>> act_df = data_collection.activity_dates_df(db, db_user, 'activity', 2848)
    >>> act_df.date[1]
    Timestamp('2015-01-19 22:27:33.266120')
    '''
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = 'SELECT date FROM {} WHERE user_id = {};'.format(table, int(app_user))
    cur.execute(sql)
    activity_dates = pd.DataFrame(cur.fetchall(),
    columns= ['date']).sort(['date'], ascending=True)
    next_activity = activity_dates['date'][1:]
    activity_dates = activity_dates[:len(activity_dates)-1]
    next_activity.index = activity_dates.index
    activity_dates['next'] = next_activity
    activity_dates['dif_time'] = activity_dates['next']-activity_dates['date']
    conn.close()
    return activity_dates

def user_stats_df(db, db_user, table):
    ''' Create a Pandas DataFrame containing the activity statistics for a list
    of users.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry

    Returns
    -------
    DataFrame containing the activity statistics.

    Example
    -------
    >>> stats_df = data_collection.user_stats_df(db, db_user, 'activity')
    >>> len(stats_df)
    13588
    '''
    user_stats = pd.DataFrame(columns= ['app_user','first_use', 'last_use', 'time_with_app', 'num_uses',
                                         'min_away', 'max_away', 'avg_away', 'median_away'])
    user_table = user_df_maker(db, db_user, table)
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
    '''
    Pull user_stats table from database.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database

    Returns
    -------
    DataFrame containing data from user_stats table which was added to the the
    database.

    Example
    -------
    '''
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql ='''SELECT app_user, first_use, last_use, time_with_app, num_uses,
    min_away, max_away, avg_away, median_away
    FROM user_stats;
    '''
    cur.execute(sql)
    user_stats = pd.DataFrame(cur.fetchall(), columns= ['app_user', 'first_use',
                                                        'last_use',
                                                        'time_with_app',
                                                        'num_uses', 'min_away',
                                                        'max_away', 'avg_away',
                                                        'median_away'])
    conn.close()
    user_stats.time_with_app = pd.to_timedelta(user_stats.time_with_app, unit='ns')
    user_stats.min_away = pd.to_timedelta(user_stats.min_away, unit='ns')
    user_stats.max_away = pd.to_timedelta(user_stats.max_away, unit='ns')
    user_stats.avg_away = pd.to_timedelta(user_stats.avg_away, unit='ns')
    user_stats.median_away = pd.to_timedelta(user_stats.median_away, unit='ns')
    return user_stats

def create_activity_table(db, db_user):
    '''
    Create a table in the PostgreSQL data base containing user activity from
    multiple tables.
    This fuction is used to classify users as active and inactive during
    different time periods.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database

    Returns
    -------
    table size

    '''
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
    '''
    Count the users that are active beyond a date.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    last_date: string formated like '2016-03-01'

    Returns
    -------
    count
    '''
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
    '''
    Save tables that are timeconsuming to creat in postgreSQL DB
    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to create

    Returns
    -------
    nothing

    Example
    -------
    '''
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
    sql = '''
    SELECT date, id
    FROM {}
    WHERE user_id = {}
    AND date > '{}'
    AND date < '{}';'''.format(table, int(app_user), date1, date2)
    cur.execute(sql)
    activity_dates = pd.DataFrame(cur.fetchall(),
                                    columns= ['date', 'id']).sort(['date'],
                                    ascending=True)
    conn.close()
    activity_dates = activity_dates.reset_index(drop=True)
    return activity_dates
