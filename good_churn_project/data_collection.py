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
    next_activity = activity_dates['date'].ix[1:]
    activity_dates = activity_dates.ix[:len(activity_dates)-2]
    next_activity.index = activity_dates.index
    activity_dates['next'] = next_activity
    activity_dates['dif_time'] = activity_dates['next']-activity_dates['date']
    conn.close()
    return activity_dates 
