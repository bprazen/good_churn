import pandas as pd
import psycopg2 as pg2
import data_collection

def select_churn_users(user_stats, time_away, minimum_total_uses):
    '''
    Selects all users that have churned given a DataFrame containing the
    user_stats this includes good churn and bad churn.

    Parameters
    ----------
    user_stats: pandas DataFrame created using data_collection.user_stats_df
    time_away: int of days
    minimum_total_uses: int of the minimum number of activities that will
    qualify.

    Returns
    -------
    churn_users: pandas DataFrame

    Example
    -------
    '''
    churn_users = user_stats[(user_stats.max_away > pd.tslib.Timedelta(days=time_away))
                                & (user_stats.num_uses > minimum_total_uses)]
    churn_users = churn_users.reset_index(drop=True)
    return churn_users

def idendify_good_churn_user(db, db_user, table, app_user, leave_time,
                                prechurn_time, prechurn_act, postchurn_time,
                                postchurn_act):
    '''
    Create a Pandas DataFrame containing the time regions that qualify
    as good churn for a user given the leave_time (days), prechurn_time (days),
    prechurn activity postchurn_time (days) and postchurn activity.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry
    app_user: string containing the user_id
    leave_time: int containing the number of days without activity
    prechurn_time: int containing the number of prechurn days to monitor for
    activity.
    prechurn_act: int containing the number of activities in the prechurn_time
    postchurn_time: int containing the number of postchurn days to monitor for
    activity.
    postchurn_act: int containing the number of activities in the postchurn_time

    Returns
    -------
    good_churns: pandas DataFrame

    Example
    -------
    '''

    act_one_user = data_collection.activity_dates_df(db, db_user, table, app_user)
    activity_on_leaving = act_one_user[act_one_user.dif_time > pd.tslib.Timedelta(days=leave_time)]
    activity_on_leaving = activity_on_leaving.reset_index(drop=True)
    good_churns = pd.DataFrame(columns= ['user_id', 'churn_date',
                                            'No_prechurn_activities',
                                            'No_postchurn_activities',
                                            'dif_time'])
    for index, row in activity_on_leaving.iterrows():
        date1 = str(row.date.date()-pd.tslib.Timedelta(days=14))
        date2 = str(row.date.date())
        # calculate prechurn activity for a time-window
        pre_leave_df = data_collection.activity_dates_range_df(db, db_user,
                                                                table, app_user,
                                                                 date1, date2)
        date1 = str(row.next.date())
        date2 = str(row.next.date()+pd.tslib.Timedelta(days=postchurn_time))
        # calculate postchurn activity for a time-window
        post_leave_df = data_collection.activity_dates_range_df(db, db_user,
                                                                table, app_user,
                                                                date1, date2)
        if len(pre_leave_df) > prechurn_act and len(post_leave_df) > postchurn_act:
            series = pd.Series([app_user, row.date.date(), len(pre_leave_df),
                                len(post_leave_df), row.dif_time],
                                index=['user_id', 'churn_date',
                                        'No_prechurn_activities',
                                        'No_postchurn_activities', 'dif_time'])
            good_churns = good_churns.append(series, ignore_index=True)
    return good_churns


def idendify_good_churn_across_user(db, db_user, table, user_ids, leave_time,
                                    prechurn_time, prechurn_act, postchurn_time,
                                    postchurn_act):
    '''
    Create a Pandas DataFrame containing the time regions that qualify
    as good churn for a list of users given the user_ids, leave_time (days),
    prechurn_time (days), prechurn activity
    postchurn_time (days) and postchurn activity.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry
    user_ids: list of user_ids to scan for good churn
    leave_time: int containing the number of days without activity
    prechurn_time: int containing the number of prechurn days to monitor for
    activity.
    prechurn_act: int containing the number of activities in the prechurn_time
    postchurn_time: int containing the number of postchurn days to monitor for
    activity.
    postchurn_act: int containing the number of activities in the postchurn_time

    Returns
    -------
    good_churns: pandas DataFrame

    Example
    -------
    '''
    good_churns = pd.DataFrame(columns= ['user_id', 'churn_date',
    'No_prechurn_activities', 'No_postchurn_activities', 'dif_time'])
    for user_id in user_ids:
        churn = idendify_good_churn_user(db, db_user, table, user_id,
                                            leave_time, prechurn_time,
                                            prechurn_act, postchurn_time,
                                            postchurn_act)
        good_churns = good_churns.append(churn, ignore_index=True)
    return good_churns


def idendify_bad_churn_users(db, db_user, table, time_gone, prechurn_act):
    '''
    Create a Pandas DataFrame containing the time users that qualify
    as bad churn given the user_ids, time_gone (days) and  prechurn activity.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry
    time_gone: int containing the number of days without activity
    prechurn_act: int containing the number of activities required

    Returns
    -------
    bad_churn: pandas DataFrame

    Example
    -------

    '''
    date_of_leave = pd.tslib.Timestamp('2016-04-04')-pd.tslib.Timedelta(days=time_gone)
    date_of_leave = str(date_of_leave.date())
    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''
    WITH churn AS
    (
    SELECT user_id, MAX(date) AS last_day, count(date) AS activity_count,
    (MAX(date) > '{}') AS still_in
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
    bad_churn = pd.DataFrame(cur.fetchall(), columns= ['user_id', 'last_day',
                                                        'activity_count'])
    conn.close()
    return bad_churn


def idendify_test_users(db, db_user, table, time_gone_low, time_gone_high, prechurn_act):
    '''
    Create a Pandas DataFrame containing users that will serve as test data
    given a time window of interest and prechurn activity.

    Parameters
    ----------
    db: string containing name of local postgreSQL data base
    db_user: string containing the user name for login to database
    table: string containing the database table to querry
    time_gone_low: int containing the least number of days without activity
    time_gone_high: int cntaining the most days withouth activity
    prechurn_act: int containing the number of activities required

    Returns
    -------
    test_data: pandas DataFrame

    Example
    -------

    '''
    date__gone_high = pd.tslib.Timestamp('2016-04-04')-pd.tslib.Timedelta(days=time_gone_low)
    date__gone_low = pd.tslib.Timestamp('2016-04-04')-pd.tslib.Timedelta(days=time_gone_high)
    date__gone_high = str(date__gone_high.date())
    date__gone_low = str(date__gone_low.date())

    conn = pg2.connect(dbname=db, user=db_user, host='localhost')
    cur = conn.cursor()
    sql = '''WITH test AS
    (
    SELECT user_id, MAX(date) AS last_day,
    count(date) AS activity_count,
    (MAX(date) > '{}' AND MAX(date) < '{}') AS in_window
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
    test_data = pd.DataFrame(cur.fetchall(), columns= ['user_id', 'last_day',
                                                        'activity_count'])
    conn.close()
    return test_data

def add_class_to_df(df, good_churn_user_id):
    '''
    Appends a class column to a DataFrame that contains a user_id column, given
    a list of user_id's for the 1 class

    Parameters
    ----------
    df: pandas DataFrame contining user_ids which class labels will be added to
    good_churn_user_id: user_ids to be asigned class = 1

    Returns
    -------
    df: pandas DataFrame with class column appended

    Example
    -------

    '''
    c = np.zeros(len(df.user_id))
    for index, user_id in enumerate(df.user_id):
        if user_id in good_churn_user_id:
            c[index] = 1
    df['class'] = c
    return df
