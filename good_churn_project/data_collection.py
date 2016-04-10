import psycopg2 as pg2
import pandas as pd

# note: need to determine how to deal with tables that have user_id insted of sender_id
def user_df(db, db_user, table):
    # Create a Pandas DataFrame containing the user_id, first activity (start_date)
    # latest activity (stop_date)
    #
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql ='''
    SELECT sender_id, MIN(created_at) AS start_date, MAX(created_at) AS stop_date
    FROM {}
    GROUP BY sender_id
    ;
    '''.format(table)
    cur.execute(sql)
    user_table = pd.DataFrame(cur.fetchall(), columns= ['user_id', 'start_date', 'stop_date'])
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
