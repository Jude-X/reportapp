import pandas as pd
import base64
import streamlit as st
import datetime
import plotly.graph_objects as go
import numpy as np
import calendar
from calendar import monthrange
from gsheet import gsheet_api_check, pull_sheet_data
import pandas.io.sql as psql


# Daily Report Functions
def today_dates(c):
    c.execute('''SELECT MAX(Date) FROM datatable''')
    latestdate = c.fetchone()[0]

    with st.beta_expander(""):
        today1 = st.date_input(
            'Date', latestdate, min_value=datetime.datetime(2020, 1, 1), max_value=latestdate)

    today = today1.strftime("%d-%b-%Y")
    todaystr = today1.strftime("%a")
    return today1, today, todaystr


def yesterday_dates(today1):
    '''
    Gets yesterday's dates in datetime, str and int
    '''
    yesterday1 = today1 - datetime.timedelta(days=1)
    yest = yesterday1.strftime("%d-%b-%Y")
    yesstr = yesterday1.strftime("%a")
    return yesterday1, yest, yesstr


def week_dates(today1):
    '''
    Gets week's dates in int
    '''
    thisweek = int(today1.strftime("%V"))
    lastweek = int((today1 -
                    datetime.timedelta(weeks=1)).strftime("%V"))
    if lastweek > thisweek:
        lastweekyear = (today1 -
                        datetime.timedelta(weeks=1)).year
    else:
        lastweekyear = today1.year
    return thisweek, lastweek, lastweekyear


def month_dates(today1):
    '''
    Gets month's dates in datetime, str and int
    '''
    lastmonth1 = today1.replace(day=1) - datetime.timedelta(days=1)
    lastmonth = lastmonth1.strftime("%B")
    month = today1.strftime('%B')
    thismonth = int(today1.strftime("%m"))
    return lastmonth1, lastmonth, month, thismonth


def year_dates(today1):
    '''
    Gets number of days in a month, days left in a year and days in a year
    '''
    year = today1.year
    numofdays = monthrange(today1.year, today1.month)[1]
    if today1.month == 1:
        lastnumofdays = monthrange(today1.year, 12)[1]
    else:
        lastnumofdays = monthrange(today1.year, today1.month-1)[1]
    daysinyr = 366 if calendar.isleap(today1.year) else 365
    daysleft = daysinyr - int(today1.strftime('%j'))
    return year, numofdays, lastnumofdays, daysinyr, daysleft


def color_change(val):
    '''
    this function is used for conditional formatting of the dfsum and dfmtd dataframes
    '''
    if isinstance(val, int) or isinstance(val, float):
        if val > 0:
            color = 'green'
        elif val < 0:
            color = 'red'
        else:
            color = 'black'
    else:
        color = 'black'
    return 'color: %s' % color


def pro_color_change(val):
    '''
    this function is used for conditional formatting of the projection dataframe
    '''
    if isinstance(val, int) or isinstance(val, float):
        if val > 99:
            color = 'green'
        elif val < 100:
            color = 'red'
        else:
            color = 'black'
    else:
        color = 'black'
    return 'color: %s' % color


def df_sum(conn, today1, yesterday1, todaystr, today, yesstr, yest):
    sqltoday = today1.strftime("%Y-%m-%d")
    sqlyesterday = yesterday1.strftime("%Y-%m-%d")
    dfsum = psql.read_sql('''
                        WITH t1 AS (
                        SELECT product AS Product, 
                        date,
                        SUM("tpv$") TPV$,
                        SUM("rev$") Rev$,
                        SUM("tpc") TPC
                        FROM datatable
                        WHERE date IN (%(s1)s,%(s2)s)
                        GROUP BY 1, 2),
                        
                        t2 AS (
                        SELECT vertical AS Product, 
                        date,
                        SUM("tpv$") TPV$,
                        SUM("rev$") Rev$,
                        SUM("tpc") TPC
                        FROM datatable
                        WHERE date IN (%(s1)s,%(s2)s) AND vertical = 'Agency'
                        GROUP BY 1, 2)
                        
                        SELECT *
                        FROM t1
                        UNION
                        SELECT *
                        FROM t2
                        ''',
                          conn, params={'s1': sqltoday, 's2': sqlyesterday})  # parse_dates='date')

    dfsum.columns = ['Product', 'Date', 'TPV$', 'Rev$', 'TPC']
    dfsum = pd.melt(dfsum, id_vars=['Product', 'Date'], value_vars=[
        'TPV$', 'Rev$', 'TPC'], var_name='Metrics')
    dfsum = dfsum.pivot_table(
        index=['Product', 'Metrics'], columns='Date', values='value').reset_index()
    dfsum['Variance'] = (dfsum.iloc[:, -1] -
                         dfsum.iloc[:, -2])/dfsum.iloc[:, -2]
    colname = dfsum.columns.tolist()
    colname[-2], colname[-3] = (f'{todaystr}, {today}',
                                f'{yesstr}, {yest}')
    dfsum.columns = colname
    return dfsum


def mtd(conn, today1, lastmonth, lastmonth1, numofdays, lastnumofdays, team_name=None):
    sqllastmonth = int(lastmonth1.strftime("%m"))
    sqllastmonthyear = int(lastmonth1.strftime("%Y"))
    sqlthismonth = int(today1.strftime("%m"))
    sqlthismonthyear = int(today1.strftime("%Y"))
    sqltodayday = int(today1.day)

    if not team_name:
        dfmtd = psql.read_sql('''
                            WITH t1 AS (
                            SELECT product AS Product, 
                            month,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s2)s AND year = %(s3)s 
                            GROUP BY 1, 2),
                            
                            t2 AS (
                            SELECT product AS Product, 
                            month,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s4)s AND year = %(s5)s 
                            GROUP BY 1, 2)
                            
                            SELECT *
                            FROM t1
                            UNION
                            SELECT *
                            FROM t2
                            ''',
                              conn, params={'s1': sqltodayday, 's2': sqlthismonth, 's3': sqlthismonthyear, 's4': sqllastmonth, 's5': sqllastmonthyear})

        dfsumrun = psql.read_sql('''
                            SELECT day,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s2)s AND year = %(s3)s 
                            GROUP BY 1
                            ''',
                                 conn, params={'s1': sqltodayday, 's2': sqlthismonth, 's3': sqlthismonthyear})

        dfsumrunlast = psql.read_sql('''
                            SELECT day,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s4)s AND year = %(s5)s 
                            GROUP BY 1
                            ''',
                                     conn, params={'s1': sqltodayday, 's4': sqllastmonth, 's5': sqllastmonthyear})
    else:
        dfmtd = psql.read_sql('''
                            WITH t1 AS (
                            SELECT product AS Product, 
                            month,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s2)s AND year = %(s3)s AND vertical = %(s6)s
                            GROUP BY 1, 2),
                            
                            t2 AS (
                            SELECT product AS Product, 
                            month,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s4)s AND year = %(s5)s AND vertical = %(s6)s
                            GROUP BY 1, 2)
                            
                            SELECT *
                            FROM t1
                            UNION
                            SELECT *
                            FROM t2
                            ''',
                              conn, params={'s1': sqltodayday, 's2': sqlthismonth, 's3': sqlthismonthyear, 's4': sqllastmonth, 's5': sqllastmonthyear, 's6': team_name})

        dfsumrun = psql.read_sql('''
                            SELECT day,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s2)s AND year = %(s3)s AND vertical = %(s6)s
                            GROUP BY 1
                            ''',
                                 conn, params={'s1': sqltodayday, 's2': sqlthismonth, 's3': sqlthismonthyear, 's6': team_name})

        dfsumrunlast = psql.read_sql('''
                            SELECT day,
                            SUM("rev$") Rev$
                            FROM datatable
                            WHERE day <= %(s1)s AND month = %(s4)s AND year = %(s5)s AND vertical = %(s6)s
                            GROUP BY 1
                            ''',
                                     conn, params={'s1': sqltodayday, 's4': sqllastmonth, 's5': sqllastmonthyear, 's6': team_name})
    dfmtd.columns = ['Product', 'Month', 'Rev$']
    dfsumrun.columns = dfsumrunlast.columns = ['Day', 'Rev$']
    dfmtd = dfmtd.pivot_table(
        index=['Product'], columns='Month', values='Rev$').reset_index()
    dfmtd['Variance'] = (dfmtd.iloc[:, -1] -
                         dfmtd.iloc[:, -2])/dfmtd.iloc[:, -2]
    dfmtd.columns = ['Product', f'{lastmonth[0:3]} MTD',
                     f'{today1.strftime("%B")[0:3]} MTD', 'Variance']
    mtdsumthis = round(dfmtd[f'{today1.strftime("%B")[0:3]} MTD'].sum(), 2)
    mtdsumlast = round(dfmtd[f'{lastmonth[0:3]} MTD'].sum(), 2)
    dfsumrun['Avg1'] = dfsumrun['Rev$'].rolling(window=4).mean()
    dfsumrun = dfsumrun.assign(Avg1=np.where(
        dfsumrun.shape[0] < 4, dfsumrun['Rev$'].mean(), dfsumrun.Avg1))
    runrate = round(dfsumrun['Avg1'].mean()*numofdays, 2)
    dfsumrunlast['Avg1'] = dfsumrunlast['Rev$'].rolling(window=4).mean()
    dfsumrunlast = dfsumrunlast.assign(Avg1=np.where(
        dfsumrunlast.shape[0] < 4, dfsumrunlast['Rev$'].mean(), dfsumrunlast.Avg1))
    runratelast = round(dfsumrunlast['Avg1'].mean()*lastnumofdays, 2)
    return mtdsumthis, mtdsumlast, runrate, runratelast, dfmtd


def ytd(conn, today1, daysleft, team_name=None):
    sqlthismonthyear = int(today1.strftime("%Y"))

    if not team_name:
        dfyrsumrun = psql.read_sql('''
                        SELECT date,
                        SUM("rev$") Rev$
                        FROM datatable
                        WHERE year = %(s3)s 
                        GROUP BY 1
                        ''',
                                   conn, params={'s3': sqlthismonthyear})
    else:
        dfyrsumrun = psql.read_sql('''
                        SELECT date,
                        SUM("rev$") Rev$
                        FROM datatable
                        WHERE year = %(s3)s AND vertical =  %(s6)s 
                        GROUP BY 1
                        ''',
                                   conn, params={'s3': sqlthismonthyear, 's6': team_name})
    dfyrsumrun.columns = ['Date', 'Rev$']
    ytdsum = round(dfyrsumrun['Rev$'].sum(), 2)
    dfyrsumrun['Avg1'] = dfyrsumrun['Rev$'].rolling(window=4).mean()
    dfyrsumrun = dfyrsumrun.assign(Avg1=np.where(
        dfyrsumrun.shape[0] < 4, dfyrsumrun['Rev$'].mean(), dfyrsumrun.Avg1))
    fyrunrate = round((dfyrsumrun['Avg1'].mean()*daysleft)+ytdsum, 2)
    return ytdsum, fyrunrate


def get_table_download_link(df, name):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(
        csv.encode()
    ).decode()  # some strings <-> bytes conversions necessary here
    return f'<a href="data:file/csv;base64,{b64}" download="{name}.csv"> Download {name} </a>'


def daily_product_notes(conn, today1, yesterday1, yesstr, yest, todaystr, today, metric, no_of_merch, product_selected=['Collections', 'Payouts', 'FX']):
    sqltoday = today1.strftime("%Y-%m-%d")
    sqlyesterday = yesterday1.strftime("%Y-%m-%d")

    dfdn = pd.DataFrame(columns=['Product', 'MerchName2', 'Currency',
                                 f'{yesstr}, {yest}', f'{todaystr}, {today}', 'Variance', '% of Rise/Drop'])
    for product in product_selected:

        qthis = f'''
            SELECT product, 
            REPLACE(merchname2,'Critical Ideas, Inc.','Chipper Cash App') AS merchname2,
            currency,
            SUM({metric.lower()})
            FROM datatable
            WHERE date = '{sqltoday}' AND product = '{product}'
            GROUP BY 1,2,3
            ORDER BY 4
            '''

        qlast = f'''SELECT product, 
            REPLACE(merchname2,'Critical Ideas, Inc.','Chipper Cash App')AS merchname2,
            currency,
            SUM({metric.lower()})
            FROM datatable
            WHERE date = '{sqlyesterday}' AND product = '{product}'
            GROUP BY 1,2,3
            ORDER BY 4
            '''
        dfmernoteThis = psql.read_sql(qthis, conn)
        dfmernoteLast = psql.read_sql(qlast, conn)
        dfmernoteThis.columns = dfmernoteLast.columns = [
            'Product', 'MerchName2', 'Currency', metric]
        dfdailynote = dfmernoteLast.merge(
            dfmernoteThis, on=['Product', 'MerchName2', 'Currency'], how='outer')
        dfdailynote.fillna(0, inplace=True)
        dfdailynote['Variance'] = dfdailynote[f'{metric}_y'] - \
            dfdailynote[f'{metric}_x']
        dfdailynote = dfdailynote.sort_values('Variance', ascending=[False])
        dfdailynote = dfdailynote.reset_index(drop=True)
        dfdailynote['% of Rise/Drop'] = np.where(dfdailynote.Variance < 0, -dfdailynote.Variance/dfdailynote[(
            dfdailynote.Variance < 0)]['Variance'].sum(), dfdailynote.Variance*100/dfdailynote[dfdailynote.Variance > 0]['Variance'].sum())
        dfdailynote.rename(columns={f'{metric}_x': f'{yesstr}, {yest}',
                                    f'{metric}_y': f'{todaystr}, {today}'}, inplace=True)
        result = dfdailynote.iloc[(dfdailynote.index < no_of_merch) | (
            dfdailynote.index > len(dfdailynote)-no_of_merch-1)]
        dfdn = dfdn.append(result)
        dfdn = dfdn.reset_index(drop=True)

    return dfdn


def projection(conn, dfpro, numofdays, today1):
    sqlthismonth = int(today1.strftime("%m"))
    sqlthismonthyear = int(today1.strftime("%Y"))
    df = psql.read_sql('''
                        SELECT REPLACE(merchname2,'Critical Ideas, Inc.','Chipper Cash App') AS merchname2,
                        SUM("rev$") actual
                        FROM datatable
                        WHERE month = %(s2)s AND year = %(s3)s AND merchname2 IN %(s4)s
                        GROUP BY 1
                        ORDER BY 2 DESC
                        ''',
                       conn, params={'s2': sqlthismonth, 's3': sqlthismonthyear, 's4': tuple(dfpro.MerchName2.tolist())})
    df.columns = ['MerchName2', 'Actual']
    df['Actual'] = df['Actual'].map(lambda x: round(x, 2))
    dfpro['Proj MTD'] = dfpro.bestCase.map(
        lambda x: round(x*(today1.day)/numofdays, 2))
    df1 = dfpro.merge(df, on='MerchName2', how='inner')
    df1['% Achieved'] = df1['Actual']/df1['Proj MTD'].map(lambda x: float(x))
    df1['% Achieved'] = df1['% Achieved'].map(lambda x: round(x, 2))
    df1 = df1.sort_values('Proj MTD', ascending=False).reset_index(drop=True)
    return df1


# Weekly Report Functions

def week_summary(conn, today1, year, lastweekyear, thisweek, lastweek, thismonth, lastmonth, numofdays):
    dfweek = psql.read_sql('''
                    SELECT product,
                    SUM("rev$") Rev$,
                    SUM("tpv$") TPV$,
                    SUM("tpc") TPC
                    FROM datatable
                    WHERE year = %(s3)s AND week = %(s4)s
                    GROUP BY 1
                        ''',
                           conn, params={'s3': year, 's4': thisweek})

    dfweeklastyr = psql.read_sql('''
                    SELECT product,
                    SUM("rev$") Rev$,
                    SUM("tpv$") TPV$,
                    SUM("tpc") TPC
                    FROM datatable
                    WHERE year = %(s3)s AND week = %(s4)s
                    GROUP BY 1
                        ''',
                                 conn, params={'s3': year, 's4': thisweek})

    dflastweek = psql.read_sql('''
                    SELECT product,
                    SUM("rev$") Rev$,
                    SUM("tpv$") TPV$,
                    SUM("tpc") TPC
                    FROM datatable
                    WHERE year = %(s3)s AND week = %(s4)s
                    GROUP BY 1
                        ''',
                               conn, params={'s3': lastweekyear, 's4': lastweek})

    dfweeklyrev = psql.read_sql('''
                    SELECT week,
                    SUM("rev$") Rev$
                    FROM datatable
                    WHERE year = %(s3)s
                    GROUP BY 1
                        ''',
                                conn, params={'s3': year})

    dfsumrun = psql.read_sql('''
                        SELECT day,
                        SUM("rev$") Rev$
                        FROM datatable
                        WHERE day <= %(s1)s AND month = %(s2)s AND year = %(s3)s 
                        GROUP BY 1
                        ''',
                             conn, params={'s1': today1.day, 's2': thismonth, 's3': year})

    totmonthrev = psql.read_sql('''
                    SELECT SUM("rev$") Rev$
                    FROM datatable
                    WHERE year = %(s3)s AND month = %(s4)s
                        ''',
                                conn, params={'s3': year, 's4': thismonth})

    dfsumrun.columns = ['Day', 'Rev$']
    dfweek.columns = dflastweek.columns = dfweeklastyr.columns = [
        'Product', 'Rev$', 'TPV$', 'TPC']
    dfweeklyrev.columns = ['Week', 'Rev$']
    totmonthrev = totmonthrev['rev$'].item()
    df1 = dfweek.loc[:, 'Product':'Rev$']
    df1['Product'] = pd.Categorical(df1['Product'], categories=[
        'Collections', 'Payouts', 'FX', 'Barter', 'Others'], ordered=True)
    df1.sort_values('Product', inplace=True)
    df1 = df1.reset_index(drop=True)
    df2 = dflastweek.loc[:, 'Product':'Rev$']
    df2['Product'] = pd.Categorical(df2['Product'], categories=[
        'Collections', 'Payouts', 'FX', 'Barter', 'Others'], ordered=True)
    df2.sort_values('Product', inplace=True)
    df2 = df2.reset_index(drop=True)
    df3 = dfweeklastyr.loc[:, 'Product':'Rev$']
    df3['Product'] = pd.Categorical(df3['Product'], categories=[
        'Collections', 'Payouts', 'FX', 'Barter', 'Others'], ordered=True)
    df3.sort_values('Product', inplace=True)
    df3 = df3.reset_index(drop=True)
    dfweeklastyr['Product'] = pd.Categorical(dfweeklastyr['Product'], categories=[
        'Collections', 'Payouts', 'FX', 'Barter', 'Others'], ordered=True)
    dfweeklastyr.sort_values('Product', inplace=True)
    dfweeklastyr = dfweeklastyr.reset_index(drop=True)
    dfweeksum = df1.merge(df2, on='Product')
    dfweeklyrev.iloc[:, 1] = dfweeklyrev.iloc[:, 1].map(lambda x: round(x, 2))
    # Week Summary -  Revenue, TPV, Revenue Increase and Run Rate
    weekrevsL = dflastweek['Rev$'].sum()
    weekrevs = dfweek['Rev$'].sum()
    weekTPVsL = dflastweek['TPV$'].sum()
    weekTPVs = dfweek['TPV$'].sum()
    weekTPCL = dflastweek['TPC'].sum()
    weekTPC = dfweek['TPC'].sum()
    # Week Summary - Target Achieved
    # Week Summary - runrate
    dfsumrun['Avg1'] = dfsumrun['Rev$'].rolling(window=4).mean()
    dfsumrun = dfsumrun.assign(Avg1=np.where(
        dfsumrun.shape[0] < 4, dfsumrun['Rev$'].mean(), dfsumrun.Avg1))
    monrunrate = dfsumrun['Avg1'].mean()*numofdays
    weekStat = [weekrevs, weekTPVs, weekTPC, totmonthrev, monrunrate]
    weekStat2 = [weekrevsL, weekTPVsL, weekTPCL]
    return df1, df2, df3, dfweek, dfweeklastyr, dflastweek, dfweeksum, weekStat, dfweeklyrev, dfweeksum, weekStat2


def week_exfx_summary(conn, today1, year, dfweek, dflastweek, thisweek, thismonth, numofdays):
    # Ex FX
    dfweeklyrevexFX = psql.read_sql('''
                    SELECT week,
                    SUM("rev$") Rev$
                    FROM datatable
                    WHERE year = %(s3)s AND product != 'FX'
                    GROUP BY 1
                        ''',
                                    conn, params={'s3': year})

    totmonthrevexFX = psql.read_sql('''
                    SELECT SUM("rev$") Rev$
                    FROM datatable
                    WHERE year = %(s3)s AND month = %(s4)s AND product != 'FX'
                        ''',
                                    conn, params={'s3': year, 's4': thismonth})

    dfsumrunexFX = psql.read_sql('''
                        SELECT day,
                        SUM("rev$") Rev$
                        FROM datatable
                        WHERE day <= %(s1)s AND 
                        month = %(s2)s AND 
                        year = %(s3)s AND 
                        product != 'FX'
                        GROUP BY 1
                        ''',
                                 conn, params={'s1': today1.day, 's2': thismonth, 's3': year})

    dfsumrunexFX.columns = ['Day', 'Rev$']
    totmonthrevexFX = totmonthrevexFX['rev$'].item()
    dfweeklyrevexFX.columns = ['Week', 'Rev$']
    weekrevFX = dfweek[dfweek['Product'] != 'FX']['Rev$'].sum()
    weekrevFXL = dflastweek[dflastweek['Product'] != 'FX']['Rev$'].sum()
    weekTPVFX = dfweek[dfweek['Product'] != 'FX']['TPV$'].sum()
    weekTPVFXL = dflastweek[dflastweek['Product'] != 'FX']['TPV$'].sum()
    weekTPCFX = dfweek[dfweek['Product'] != 'FX']['TPC'].sum()
    weekTPCFXL = dflastweek[dflastweek['Product'] != 'FX']['TPC'].sum()
    # Week exFX Summary - Target Achieved

    # Week exFX Summary - runrate
    dfsumrunexFX['Avg1'] = dfsumrunexFX['Rev$'].rolling(window=4).mean()
    monrunrateexFX = dfsumrunexFX['Rev$'].mean()*numofdays
    weekStatexFX = [weekrevFX, weekTPVFX,
                    weekTPCFX, totmonthrevexFX, monrunrateexFX]
    weekStatexFX2 = [weekrevFXL, weekTPVFXL, weekTPCFXL]
    return dfweeklyrevexFX, weekStatexFX, weekStatexFX2


def week_colpay_summary(product, conn, year, dfweek, dflastweek, thisweek):
    # Collections and Payout Performance
    dfweekColpay = psql.read_sql('''
                SELECT week,
                SUM("rev$") Rev$,
                SUM("tpv$") TPV$,
                SUM("tpc") TPC
                FROM datatable
                WHERE year = %(s3)s AND product = %(s4)s
                GROUP BY 1
                       ''',
                                 conn, params={'s3': year, 's4': product})

    dfweekColpay.columns = ['Week', 'Rev$', 'TPV$', 'TPC']
    sumrevColpay = dfweek[dfweek.Product == product]['Rev$'].item()
    sumrevColpayL = dflastweek[dflastweek.Product == product]['Rev$'].item()
    sumTPVColpay = dfweek[dfweek.Product == product]['TPV$'].item()
    sumTPVColpayL = dflastweek[dflastweek.Product == product]['TPV$'].item()
    sumTPCColpay = dfweek[dfweek.Product == product]['TPC'].item()
    sumTPCColpayL = dflastweek[dflastweek.Product == product]['TPC'].item()
    weekStatColpay = [sumrevColpay, sumTPVColpay, sumTPCColpay]
    weekStatColpay2 = [sumrevColpayL, sumTPVColpayL, sumTPCColpayL]
    return dfweekColpay, weekStatColpay, weekStatColpay2


def week_barter_performance(conn, lastweekyear, year, thisweek, lastweek, dfweek, dflastweek):
    dfweekrevBar = psql.read_sql('''
                    SELECT week,
                    SUM("rev$") Rev$
                    FROM datatable
                    WHERE year = %(s3)s AND 
                    product = 'Barter'
                    GROUP BY 1
                        ''', conn, params={'s3': year})

    dfweektpvBar = psql.read_sql('''
                        SELECT subproduct,
                        week,
                        SUM("tpv$") TPV$
                        FROM datatable
                        WHERE year = %(s3)s AND 
                        product = 'Barter' AND
                        subproduct IN ('Airtime', 'Bills', 'Barter Send Money to Bank', 'Card Transactions','Mvisa Qr Payment', 'Card Issuance Fee', 'Pay with Barter', 'Send Money', 'Wallet Funding')     
                        GROUP BY 1,2
                            ''', conn, params={'s3': year})

    dfBarter = psql.read_sql('''
                        SELECT subproduct,
                        SUM("rev$") Rev$,
                        SUM("tpv$") TPV$, 
                        SUM("tpc") TPC
                        FROM datatable
                        WHERE year = %(s1)s AND 
                        week = %(s2)s AND
                        product = 'Barter' AND
                        subproduct IN ('Airtime', 'Bills', 'Barter Send Money to Bank', 'Card Transactions','Mvisa Qr Payment', 'Card Issuance Fee', 'Pay with Barter', 'Send Money', 'Wallet Funding')
                        GROUP BY 1
                        ''',
                             conn, params={'s1': year, 's2': thisweek})

    dfBarterLast = psql.read_sql('''
                        SELECT subproduct,
                        SUM("rev$") Rev$,
                        SUM("tpv$") TPV$, 
                        SUM("tpc") TPC
                        FROM datatable
                        WHERE year = %(s1)s AND 
                        week = %(s2)s AND
                        product = 'Barter' AND
                        subproduct IN ('Airtime','Bills','Barter Send Money to Bank', 'Card Transactions','Mvisa Qr Payment', 'Card Issuance Fee', 'Pay with Barter', 'Send Money', 'Wallet Funding')
                        GROUP BY 1
                        ''',
                                 conn, params={'s1': year, 's2': lastweek})

    dfweekrevBar.columns = ['Week', 'Rev$']
    dfweektpvBar.columns = ['Product', 'Week', 'TPV$']
    dfBarter.columns = dfBarterLast.columns = [
        'Product', 'Rev$', 'TPV$', 'TPC']
    dfB = dfBarterLast.merge(dfBarter, on='Product', how='inner')
    dfB['Rev$ Variance'] = dfB['Rev$_y'] - dfB['Rev$_x']
    #dfB['TPV$ Variance'] = dfB['TPV$_y'] - dfB['TPV$_x']
    dfB['Rev$ Variance %'] = np.where(dfB['Rev$ Variance'] < 0, dfB['Rev$ Variance']*100/dfB[dfB['Rev$ Variance'] < 0]
                                      ['Rev$ Variance'].sum(), dfB['Rev$ Variance']*100/dfB[dfB['Rev$ Variance'] > 0]['Rev$ Variance'].sum())
    #dfB['TPV$ Variance %'] = np.where(dfB['TPV$ Variance'] < 0,dfB['TPV$ Variance']*100/dfB[dfB['TPV$ Variance'] < 0]['TPV$ Variance'].sum(),dfB['TPV$ Variance']*100/dfB[dfB['TPV$ Variance'] > 0]['TPV$ Variance'].sum())
    dfB.rename(columns={'Rev$_x': f'{lastweek} Revenue', 'TPV$_x': f'{lastweek} TPV', 'TPC_x': f'{lastweek} TPC',
                        'Rev$_y': f'{thisweek} Revenue', 'TPV$_y': f'{thisweek} TPV', 'TPC_y': f'{thisweek} TPC'}, inplace=True)

    sumrevBar = dfweek[dfweek.Product == 'Barter']['Rev$'].item()
    sumrevBarL = dflastweek[dflastweek.Product == 'Barter']['Rev$'].item()
    sumTPVBar = dfweek[dfweek.Product == 'Barter']['TPV$'].item()
    sumTPVBarL = dflastweek[dflastweek.Product == 'Barter']['TPV$'].item()
    sumTPCBar = dfweek[dfweek.Product == 'Barter']['TPC'].item()
    sumTPCBarL = dflastweek[dflastweek.Product == 'Barter']['TPC'].item()
    weekStatBar = [sumrevBar, sumTPVBar, sumTPCBar]
    weekStatBar2 = [sumrevBarL, sumTPVBarL, sumTPCBarL]
    return dfB, dfweekrevBar, dfweektpvBar, weekStatBar, weekStatBar2


def pos_agency(conn, lastweekyear, thisweek, lastweek, year):
    dfagency = psql.read_sql('''
                SELECT week,
                year,
                SUM("fees") Fees, 
                SUM("rev$") Rev$,
                SUM("tpv$") TPV$,
                SUM("tpv")  TPV,
                SUM("tpc") TPC
                FROM datatable
                WHERE year IN %(s3)s AND   
                product != 'FX' AND
                vertical IN ('Agency','POS')
                GROUP BY 1,2
                       ''',
                             conn, params={'s3': tuple([year, lastweekyear])})

    dfagency.columns = ['Week', 'Year', 'Fees', 'Rev$', 'TPV$', 'TPV', 'TPC']

    dfagency1 = dfagency[(dfagency.Year == year)].groupby(
        'Week')[['Fees', 'Rev$', 'TPV$', 'TPV', 'TPC']].sum().reset_index()
    totrevAgency = dfagency[(dfagency.Year == year) & (
        dfagency.Week == thisweek)]['Rev$'].item()
    totrevAgencyL = dfagency[(dfagency.Year == lastweekyear) & (
        dfagency.Week == lastweek)]['Rev$'].item()
    tottpvAgency = dfagency[(dfagency.Year == year) & (
        dfagency.Week == thisweek)]['TPV$'].item()
    tottpvAgencyL = dfagency[(dfagency.Year == lastweekyear) & (
        dfagency.Week == lastweek)]['TPV$'].item()
    tottpcAgency = dfagency[(dfagency.Year == year) & (
        dfagency.Week == thisweek)]['TPC'].item()
    tottpcAgencyL = dfagency[(dfagency.Year == lastweekyear) & (
        dfagency.Week == lastweek)]['TPC'].item()
    weekagencyStat = [totrevAgency, tottpvAgency, tottpcAgency]
    weekagencyStat2 = [totrevAgencyL, tottpvAgencyL, tottpcAgencyL]
    return dfagency1, weekagencyStat, weekagencyStat2


def currency_performance(conn, lastweekyear, thisweek, lastweek, year):

    dfcurThisF = psql.read_sql('''
                SELECT currency,
                SUM("fees") Fees
                FROM datatable
                WHERE year = %(s3)s AND  
                week = %(s4)s AND
                currency IN %(s5)s
                GROUP BY 1
                ORDER BY 2
                       ''',
                               conn, params={'s3': year, 's4': thisweek, 's5': tuple(['USD', 'NGN', 'KES', 'EUR', 'GHS', 'ZAR', 'UGX', 'GBP', 'XAF', 'AUD'])})

    dfcurLastF = psql.read_sql('''
                SELECT currency,
                SUM("fees") Fees
                FROM datatable
                WHERE year = %(s3)s AND  
                week = %(s4)s AND 
                currency IN %(s5)s
                GROUP BY 1
                ORDER BY 2
                       ''',
                               conn, params={'s3': lastweekyear, 's4': lastweek, 's5': tuple(['USD', 'NGN', 'KES', 'EUR', 'GHS', 'ZAR', 'UGX', 'GBP', 'XAF', 'AUD'])})

    dfrevCur = psql.read_sql('''
                SELECT currency,
                week,
                SUM("rev$") Rev$
                FROM datatable
                WHERE year = %(s3)s AND  
                currency IN %(s5)s
                GROUP BY 1,2
                ORDER BY 3
                       ''',
                             conn, params={'s3': year, 's4': tuple([thisweek, lastweek]), 's5': tuple(['USD', 'NGN', 'KES', 'EUR', 'GHS', 'ZAR', 'UGX', 'GBP', 'XAF', 'AUD'])})

    dfrevCur.columns = ['Currency', 'Week', 'Rev$']
    dfcurThisF.columns = dfcurLastF.columns = ['Currency', 'Fees']
    dfcurBothF = dfcurLastF.merge(dfcurThisF, on='Currency', how='outer')
    dfcurBothF['Variance'] = round(
        (dfcurBothF['Fees_y'] - dfcurBothF['Fees_x'])/dfcurBothF['Fees_x'], 2)
    dfcurBothF.rename(columns={'Fees_y': f'Week {thisweek} Revenue',
                               'Fees_x': f'Week {lastweek} Revenue'}, inplace=True)
    dfcurBothF['Currency'] = pd.Categorical(dfcurBothF['Currency'], categories=[
                                            'USD', 'NGN', 'KES', 'EUR', 'GHS', 'ZAR', 'UGX', 'GBP', 'XAF', 'AUD'], ordered=True)
    dfcurBothF.sort_values('Currency', inplace=True)
    dfcurBothF = dfcurBothF.reset_index(drop=True)
    dfcurBothF['Currency'] = dfcurBothF['Currency'].astype('str')
    dfrevCur = dfrevCur.groupby(['Currency', 'Week'])[
        ['Rev$']].sum().reset_index()
    return dfcurBothF, dfrevCur


def currency_note(conn, year, lastweekyear, thisweek, lastweek, currency_selected):
    if currency_selected:
        dfcurnot = psql.read_sql('''
                SELECT year,
                week,
                currency,
                Product,
                merchname2,
                SUM("rev$") Rev$,
                FROM datatable
                WHERE year IN %(s3)s AND  
                week IN %(s4)s AND 
                currency IN %(s5)s
                GROUP BY 1,2,3,4,5
                ORDER BY 6
                       ''',
                                 conn, params={'s3': tuple([year, lastweekyear]), 's4': tuple([thisweek, lastweek]), 's5': tuple(currency_selected)})

        dfcurnot.columns = ['Year', 'Week', 'Currency',
                            'Product', 'MerchName2', 'Rev$']
        return dfcurnot
    else:
        pass


def cohort_analysis(conn, dfweek, year, lastweekyear, thisweek, lastweek):
    dfmerThis = psql.read_sql('''
                       SELECT REPLACE(merchname2,'Critical Ideas, Inc.','Chipper Cash App') AS merchname2,
                       SUM("rev$") Rev$
                       FROM datatable
                       WHERE year = %(s1)s AND 
                       week = %(s2)s AND
                       merchname2 NOT IN ('Barter', 'RAVE-PAYOUT') AND
                       merchname2 IS NOT NULL
                       GROUP BY 1
                       ORDER BY 2 DESC
                       ''',
                              conn, params={'s1': year, 's2': thisweek})

    dfmerLast = psql.read_sql('''
                       SELECT REPLACE(merchname2,'Critical Ideas, Inc.','Chipper Cash App') AS merchname2,
                       SUM("rev$") Rev$
                       FROM datatable
                       WHERE year = %(s1)s AND 
                       week = %(s2)s AND
                       merchname2 NOT IN ('Barter', 'RAVE-PAYOUT') AND 
                       merchname2 IS NOT NULL
                       GROUP BY 1
                       ORDER BY 2 DESC
                       ''',
                              conn, params={'s1': lastweekyear, 's2': lastweek})

    dfmerThis.columns = dfmerLast.columns = ['MerchName2', 'Rev$']
    dfcoh = dfmerThis.merge(dfmerLast, on='MerchName2', how='outer').fillna(0)
    dfcoh = dfcoh.sort_values('Rev$_x', ascending=False)
    dfcoh['Variance'] = round(
        (dfcoh['Rev$_x'] - dfcoh['Rev$_y'])/dfcoh['Rev$_y'], 2)
    dfcoh.rename(columns={'Rev$_x': f'Week {thisweek} Revenue',
                          'Rev$_y': f'Week {lastweek} Revenue'}, inplace=True)
    dfcoh = dfcoh.reset_index(drop=True)
    df50 = dfmerThis.iloc[0:50, :]
    df50 = df50.reset_index()
    summercRev = dfweek['Rev$'].sum()
    sumdf50merc = df50['Rev$'].sum()
    df20 = df50.iloc[0:20, :]
    sumdf20merc = df20['Rev$'].sum()
    df10 = df50.iloc[0:10, :]
    sumdf10merc = df10['Rev$'].sum()
    cohanalStat = [sumdf50merc, sumdf20merc, sumdf10merc, summercRev]
    return dfcoh, cohanalStat


def weekly_new_old_merch(conn, merlist, year):
    dfNew1 = psql.read_sql('''
                       SELECT merchname2,
                       week,
                       SUM("rev$") Rev$
                       FROM datatable
                       WHERE year = %(s1)s AND
                       merchname2 IN %(s2)s 
                       GROUP BY 1,2
                       ORDER BY 3 DESC
                       ''',
                           conn, params={'s1': year, 's2': tuple(merlist)})

    dfNew1.columns = ['MerchName2', 'Week', 'Rev$']

    dfNewp = pd.pivot_table(dfNew1, values='Rev$', index=[
                            'MerchName2'], columns='Week', aggfunc=np.sum).reset_index()
    dfNewp1 = pd.DataFrame(dfNewp.to_records())
    dfNewp1.drop(dfNewp1.columns[0], axis=1, inplace=True)
    dfNewp1.fillna(0, inplace=True)
    cols = dfNewp1.columns
    dfNewp = dfNewp1[['MerchName2']]
    dfNewp[cols[1]] = dfNewp1[cols[1]]
    for i in range(2, len(cols[1:])):
        dfNewp[cols[i+1]] = dfNewp1[cols[i]]-dfNewp1[cols[i-1]]
    dfNewp.fillna(0, inplace=True)
    col2dis = [cols[0], *cols[-10:]]
    return dfNewp[col2dis]


# Team Function
def team_rev(conn, year, team_name, team_month, team_quar, team_class, team_cat, team_parameter, team_merch):
    if 'All' in team_name:
        if 'All' in team_merch:
            q = f'''
                SELECT product,
                year,
                merchname2,
                classification,
                category,
                month,
                quarter,
                COALESCE(SUM("rev$"),0) rev$
                FROM datatable
                WHERE year = %(s2)s AND 
                classification IN %(s3)s AND 
                category IN %(s4)s AND 
                month IN %(s5)s AND
                quarter IN %(s6)s 
                GROUP BY 1,2,3,4,5,6,7
                ORDER BY 8 DESC
                '''
            dfmain = psql.read_sql(q, conn, params={'s2': year, 's3': tuple(team_class), 's4': tuple(team_cat), 's5': tuple(
                range(team_month[0], team_month[1]+1, 1)), 's6': tuple(range(team_quar[0], team_quar[1]+1, 1))})
            dfmain.columns = [
                'Product', 'Year', 'MerchName2', 'Classification', 'Category', 'Month', 'Quarter', 'Rev$']
            dfteamrev1 = dfmain.groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(
                by='Rev$', ascending=False).reset_index()
        elif 'All' not in team_merch:
            q = f'''
                SELECT product,
                year,
                merchname2,
                classification,
                category,
                month,
                quarter,
                COALESCE(SUM("rev$"),0) rev$
                FROM datatable
                WHERE year = %(s2)s AND 
                classification IN %(s3)s AND 
                category IN %(s4)s AND 
                month IN %(s5)s AND
                quarter IN %(s6)s AND
                merchname2 IN %(s7)s 
                GROUP BY 1,2,3,4,5,6,7
                ORDER BY 8 DESC
                '''
            dfmain = psql.read_sql(q, conn, params={'s2': year, 's3': tuple(team_class), 's4': tuple(team_cat), 's5': tuple(range(
                team_month[0], team_month[1]+1, 1)), 's6': tuple(range(team_quar[0], team_quar[1]+1, 1)), 's7': tuple(team_merch)})
            dfmain.columns = [
                'Product', 'Year', 'MerchName2', 'Classification', 'Category', 'Month', 'Quarter', 'Rev$']
            dfteamrev1 = dfmain.groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(
                by='Rev$', ascending=False).reset_index()

    elif 'All' not in team_name:
        if 'All' in team_merch:
            q = f'''
                SELECT product,
                year,
                merchname2,
                classification,
                category,
                month,
                quarter,
                COALESCE(SUM("rev$"),0) rev$
                FROM datatable
                WHERE year = %(s2)s AND 
                classification IN %(s3)s AND 
                category IN %(s4)s AND 
                month IN %(s5)s AND
                quarter IN %(s6)s AND
                vertical IN %(s7)s
                GROUP BY 1,2,3,4,5,6,7
                ORDER BY 8 DESC
                '''
            dfmain = psql.read_sql(q, conn, params={'s2': year, 's3': tuple(team_class), 's4': tuple(team_cat), 's5': tuple(
                range(team_month[0], team_month[1]+1, 1)), 's6': tuple(range(team_quar[0], team_quar[1]+1, 1)), 's7': tuple(team_name)})
            dfmain.columns = [
                'Product', 'Year', 'MerchName2', 'Classification', 'Category', 'Month', 'Quarter', 'Rev$']
            dfteamrev1 = dfmain.groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(
                by='Rev$', ascending=False).reset_index()
        elif 'All' not in team_merch:
            q = f'''
                SELECT product,
                year,
                merchname2,
                classification,
                category,
                month,
                quarter,
                COALESCE(SUM("rev$"),0) rev$
                FROM datatable
                WHERE year = %(s2)s AND 
                classification IN %(s3)s AND 
                category IN %(s4)s AND 
                month IN %(s5)s AND
                quarter IN %(s6)s AND
                merchname2 IN %(s7)s AND 
                vertical IN %(s8)s
                GROUP BY 1,2,3,4,5,6,7
                ORDER BY 8 DESC
                '''
            dfmain = psql.read_sql(q, conn, params={'s2': year, 's3': tuple(team_class), 's4': tuple(team_cat), 's5': tuple(range(
                team_month[0], team_month[1]+1, 1)), 's6': tuple(range(team_quar[0], team_quar[1]+1, 1)), 's7': tuple(team_merch), 's8': tuple(team_name)})
            dfmain.columns = [
                'Product', 'Year', 'MerchName2', 'Classification', 'Category', 'Month', 'Quarter', 'Rev$']
            dfteamrev1 = dfmain.groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(
                by='Rev$', ascending=False).reset_index()
    else:
        st.warning('wrong input')
    return dfteamrev1


# budget performance

@st.cache(ttl=3600, show_spinner=False)
def get_pipeline(sheetnames=['BUSINESS DEV_OPEYEMI FOWLER', 'IMTO_BASSEY', 'TRAVEL & HOSPITALITY_YEWANDE', 'GAMING_WALE', 'GHANA',
                             'KENYA', 'UGANDA', 'RWANDA', 'ZAMBIA', 'SK', 'EUROPE'], sheetcols=['S/N', 'PROSPECT', 'SECTOR', 'OFFERING', 'PROJECTED ANNUAL REVENUE ($)', 'ESTIMATED MONTHLY REVS ($)', 'ESTIMATED WEEKLY REVENUE ($)', 'ENTERPRISE LEVEL', 'COMMENCE TRANSACTING', 'REVENUE WEEKS AVAILABLE', 'STAGE']):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SPREADSHEET_ID = '1WUf3vNSrRD6OIRsMCgMs1mENQYfKNH42e2AwCARGvL0'
    dfpip = pd.DataFrame()
    for RANGE_NAME in sheetnames:
        data = pull_sheet_data(SCOPES, SPREADSHEET_ID, RANGE_NAME)
        df = pd.DataFrame(data[1:], columns=sheetcols)
        df['SHEET'] = RANGE_NAME
        dfpip = pd.concat([dfpip, df.reset_index(drop=True)],
                          axis=0, ignore_index=True)
    return dfpip


def process_pipeline(dfpip, team_name):
    dfpip = dfpip.reset_index(drop=True)
    col = 'SHEET'
    conditions = [dfpip[col] == 'IMTO_BASSEY',
                  dfpip[col] == 'GAMING_WALE',
                  dfpip[col] == 'GHANA',
                  dfpip[col] == 'KENYA',
                  dfpip[col] == 'UGANDA',
                  dfpip[col] == 'RWANDA',
                  dfpip[col] == 'ZAMBIA',
                  dfpip[col] == 'SK',
                  dfpip[col] == 'EUROPE',
                  dfpip[col] == 'BUSINESS DEV_OPEYEMI FOWLER',
                  dfpip[col] == 'TRAVEL & HOSPITALITY_YEWANDE'
                  ]
    choices = ['IMTO', 'Betting/Gaming', 'Ghana', 'Kenya', 'Uganda',
               'Rwanda', 'Zambia', 'Sekinat', 'Europe', 'BusinessDev', 'PSP']
    dfpip["Vertical"] = np.select(conditions, choices, default=np.nan)
    dfpip.dropna(inplace=True)
    dfpip = dfpip.reset_index(drop=True)
    if 'All' not in team_name:
        dfpip = dfpip[dfpip.Vertical.isin(team_name)]
    else:
        dfpip = dfpip
    dfpip['PROJECTED ANNUAL REVENUE ($)'] = dfpip['PROJECTED ANNUAL REVENUE ($)'].map(
        lambda x: x.replace(',', '')).str.extract('(\d+)')
    dfpip['ESTIMATED MONTHLY REVS ($)'] = dfpip['ESTIMATED MONTHLY REVS ($)'].map(
        lambda x: x.replace(',', '')).str.extract('(\d+)')
    dfpip['ESTIMATED WEEKLY REVENUE ($)'] = dfpip['ESTIMATED WEEKLY REVENUE ($)'].map(
        lambda x: x.replace(',', '')).str.extract('(\d+)')
    dfpip['PROJECTED ANNUAL REVENUE ($)'] = dfpip['PROJECTED ANNUAL REVENUE ($)'].astype(
        float)
    dfpip['ESTIMATED MONTHLY REVS ($)'] = dfpip['ESTIMATED MONTHLY REVS ($)'].astype(
        float)
    dfpip['ESTIMATED WEEKLY REVENUE ($)'] = dfpip['ESTIMATED WEEKLY REVENUE ($)'].astype(
        float)
    totexprev = dfpip['PROJECTED ANNUAL REVENUE ($)'].sum()
    numoflive = dfpip[dfpip.STAGE == 'Live']['PROSPECT'].nunique()
    expfromlive = dfpip[dfpip.STAGE ==
                        'Live']['PROJECTED ANNUAL REVENUE ($)'].sum()
    #livetarach = round(expfromlive*100/totexprev)
    dfpros = dfpip.groupby(['PROSPECT'])[['ESTIMATED MONTHLY REVS ($)',
                                          'PROJECTED ANNUAL REVENUE ($)', 'ESTIMATED WEEKLY REVENUE ($)']].sum().reset_index()
    dfpros = dfpros.sort_values(
        'ESTIMATED MONTHLY REVS ($)', ascending=False).reset_index(drop=True)
    dfstage = dfpip.groupby(['STAGE'])[['PROSPECT']].count().reset_index()
    dfstage = dfstage[dfstage.STAGE.isin(['Leads generation', 'Qualify Leads', 'Initiate contact & Identify Needs',
                                          'Agreement', 'Solutions Presentation', 'Integration', 'Negotiation', 'Pilot', 'Live'])]
    dfstage['STAGE'] = pd.Categorical(dfstage['STAGE'], categories=['Leads generation', 'Qualify Leads', 'Initiate contact & Identify Needs',
                                                                    'Agreement', 'Solutions Presentation', 'Integration', 'Negotiation', 'Pilot', 'Live'], ordered=True)
    dfstage.sort_values('STAGE', inplace=True)
    dfstage['Variance'] = dfstage.PROSPECT.map(
        lambda x: round(x*100/dfstage.PROSPECT.sum()))
    dfstage = dfstage.reset_index(drop=True)
    dfstage['STAGE'] = dfstage['STAGE'].astype('string')
    pipeStat = [totexprev, expfromlive]

    return pipeStat, numoflive, dfpros, dfstage


# Account Management Report Functions

def gainers_losers(conn, year, thismonth, team_name):
    dfmain = psql.read_sql('''
                       SELECT merchname2,
                       year,
                       month,
                       vertical,
                       COALESCE(SUM("rev$"),0) rev$
                       FROM datatable
                       GROUP BY 1,2,3,4
                       ORDER BY 5 DESC
                       ''',
                           conn)
    dfmain.columns = ['MerchName2', 'Year', 'Month', 'Vertical', 'Rev$']
    if 'All' in team_name:
        dfxx = pd.pivot_table(dfmain,
                              index='MerchName2',
                              values='Rev$',
                              columns=['Year', 'Month'],
                              fill_value=0,
                              aggfunc=np.sum,
                              )
    else:
        dfxx = pd.pivot_table(dfmain[dfmain.Vertical.isin(team_name)],
                              index='MerchName2',
                              values='Rev$',
                              columns=['Year', 'Month'],
                              fill_value=0,
                              aggfunc=np.sum,
                              )
    dfxx = dfxx.sort_values((year, thismonth), ascending=False).reset_index()
    dfxx.rename(columns={'MerchName2': 'Merchants'}, inplace=True)
    j = 3
    for i in range(0, len(dfxx.columns)+1, 4):
        try:
            dfxx.insert(j+1, f'{year} Quarter {(j+1)//4} Total',
                        dfxx.iloc[:, i+1:j+1].sum(axis=1))
            j += 4
        except:
            pass
    try:
        col2dis = dfxx.columns.tolist()
        col2dis[:] = col2dis[0:1]+col2dis[-8:]
        dfxx = dfxx[col2dis]
    except:
        pass
    for i in range(len(dfxx.columns)):
        if len(dfxx.columns[-i][0]) > 15:
            colname = dfxx.columns[-i][0]
            colname2 = dfxx.columns[-i-4][0]
            break
    try:
        dfxx['Variance'] = dfxx[colname] - dfxx[colname2]
    except:
        dfxx['Variance'] = dfxx[colname]

    dfxxgain = dfxx[dfxx['Variance'] > 0].sort_values(
        'Variance', ascending=False).reset_index(drop=True)
    dfxxloss = dfxx[dfxx['Variance'] < 0].sort_values(
        'Variance', ascending=True).reset_index(drop=True)
    return dfxxgain, dfxxloss
