import pandas as pd
import psycopg2 as pg2

def select_churn_users(user_stats, time_away, minimum_total_uses):
    '''
    Selects all users that have churned given a DataFrame containing the user_stats this includes good churn and bad churn!
    '''
    churn_users = user_stats[(user_stats.max_away > pd.tslib.Timedelta(days=time_away)) & (user_stats.num_uses > minimum_total_uses)]
    churn_users = churn_users.reset_index(drop=True)
    return churn_users

def idendify_good_churn_user(db, db_user, table, app_user, leave_time, prechurn_time, prechurn_act, postchurn_time, postchurn_act):
    '''

    Create a Pandas DataFrame containing the time regions that qualify
    as good churn for a user given the leave_time (days), prechurn_time (days), prechurn activity
    postchurn_time (days) and postchurn activity.
    '''

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
