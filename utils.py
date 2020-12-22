import pandas as pd
import base64
import streamlit as st
import datetime
import plotly.graph_objects as go
import numpy as np
import calendar
from calendar import monthrange
from gsheet import gsheet_api_check, pull_sheet_data



### Daily Report Functions

def yesterday_dates(today1):
    '''
    Gets yesterday's dates in datetime, str and int
    '''
    yesterday1 = today1 - datetime.timedelta(days=1)
    yest = yesterday1.strftime("%d-%b-%Y")
    yesstr = yesterday1.strftime("%a")
    return yesterday1, yest, yesstr 

def week_dates():
    '''
    Gets week's dates in int
    '''
    thisweek = int(datetime.datetime.today().strftime("%V"))
    lastweek = int((datetime.datetime.today() - datetime.timedelta(weeks=1)).strftime("%V"))
    if lastweek > thisweek:
        lastweekyear = (datetime.datetime.today() - datetime.timedelta(weeks=1)).year
    else:
        lastweekyear = datetime.datetime.today().year
    return thisweek, lastweek,lastweekyear

def month_dates(today1):
    '''
    Gets month's dates in datetime, str and int
    '''
    lastmonth1 = today1.replace(day=1) - datetime.timedelta(days=1)
    lastmonth = lastmonth1.strftime("%B")
    month = today1.strftime('%B')
    thismonth = int(datetime.datetime.today().strftime("%m"))
    lastmonth2 = thismonth - 1
    return lastmonth1, lastmonth, month, thismonth, lastmonth2

def year_dates(today1): 
    '''
    Gets number of days in a month, days left in a year and days in a year
    '''
    year = today1.year
    numofdays = monthrange(today1.year, today1.month)[1]
    lastnumofdays = monthrange(today1.year, today1.month-1)[1]
    daysinyr = 366 if calendar.isleap(today1.year) else 365
    daysleft = daysinyr - int(today1.strftime('%j'))
    return year, numofdays, lastnumofdays, daysinyr, daysleft

def conditions(dfmain,today1,yesterday1,lastmonth1,year):
    '''
    This function returns the boolean conditions for filtering several dataframes
    '''
    condition1 = (dfmain.Date.isin([pd.to_datetime(today1),pd.to_datetime(yesterday1)]))
    condition2 = ((dfmain.Month == lastmonth1.month) & (dfmain.Day <= today1.day) & (dfmain.Year == lastmonth1.year))
    return condition1, condition2

def df_today(dfmain, condition1):
    '''
    Returns a dataframe that consists of only today and yesterday's data
    '''
    dftoday = dfmain[condition1]
    return dftoday
    
def color_change(val):
    '''
    this function is used for conditional formatting of the dfsum and dfmtd dataframes
    '''
    if isinstance(val,int) or isinstance(val,float):
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
    if isinstance(val,int) or isinstance(val,float):
        if val > 99:
            color = 'green'
        elif val < 100:
            color = 'red'
        else:
            color = 'black'
    else:
        color = 'black'
    return 'color: %s' % color

def df_sum(dftoday,todaystr,today,yesstr,yest):
    
    dfmet = dftoday.groupby(['Product3', 'Day'])['TPV$','Rev$','TPC'].sum().reset_index()
    dfmet = pd.melt(dfmet, id_vars=['Product3','Day'], value_vars=['TPV$','Rev$','TPC'],var_name='Metrics')
    dfmet = dfmet.pivot_table(index=['Product3','Metrics'],columns='Day',values='value').reset_index()
    dfmet['Variance'] = (dfmet.iloc[:,-1] - dfmet.iloc[:,-2])/dfmet.iloc[:,-2]
    colname = dfmet.columns.tolist()
    colname[-2], colname[-3] = (f'{todaystr}, {today}', f'{yesstr}, {yest}')
    dfmet.columns = colname
    dfmet.rename(columns = {'Product3': 'Product'},inplace=True)
    dfage = dftoday.groupby(['Vertical', 'Day'])['TPV$','Rev$','TPC'].sum().reset_index()
    dfage = pd.melt(dfage, id_vars=['Vertical','Day'], value_vars=['TPV$','Rev$','TPC'],var_name='Metrics')
    dfage = dfage.pivot_table(index=['Vertical','Metrics'],columns='Day',values='value').reset_index()
    dfage['Variance'] = (dfage.iloc[:,-1] - dfage.iloc[:,-2])/dfage.iloc[:,-2]
    colname1 = dfage.columns.tolist()
    colname1[-2], colname1[-3] = (f'{todaystr}, {today}', f'{yesstr}, {yest}')
    dfage.columns = colname1
    dfage = dfage.iloc[0:3,:]
    dfage.rename(columns = {'Vertical': 'Product'},inplace=True)
    dfsum = pd.concat([dfmet,dfage]).reset_index(drop=True)
    dfsum.iloc[:,2:] = dfsum.iloc[:,2:].apply(lambda x: round(x,2))
    return dfsum

def mtd(dfmain,today1,condition2,lastmonth,numofdays,lastnumofdays):
    dfmtdthis = dfmain[(dfmain.Month == today1.month)&(dfmain.Day <= today1.day)&(dfmain.Year == today1.year)].groupby('Product3')['Rev$'].sum().reset_index()
    dfmtdthis = dfmtdthis.sort_values('Rev$', ascending=False)
    dfmtdthis = dfmtdthis.reset_index(drop=True)
    mtdsumthis = round(dfmtdthis['Rev$'].sum(),2)
    dfmtdlast = dfmain[condition2].groupby('Product3')['Rev$'].sum().reset_index()
    dfmtdlast = dfmtdlast.sort_values('Rev$', ascending=False)
    dfmtdlast = dfmtdlast.reset_index(drop=True)
    mtdsumlast = round(dfmtdlast['Rev$'].sum(),2)
    dfmtd = dfmtdlast.merge(dfmtdthis, on='Product3', how='inner')
    dfmtd.rename(columns={'Rev$_y':f'{today1.strftime("%B")} MTD','Rev$_x':f'{lastmonth} MTD'}, inplace=True)
    dfmtd['Variance'] = (dfmtd.iloc[:,-1] - dfmtd.iloc[:,-2])/dfmtd.iloc[:,-2]
    dfmtd.iloc[:,1:] = dfmtd.iloc[:,1:].apply(lambda x: round(x,2))
    dfmtd.rename(columns = {'Product3': 'Product'},inplace=True)
    dfsumrun = dfmain[(dfmain.Month == today1.month)&(dfmain.Day <= today1.day)&(dfmain.Year == today1.year)].groupby('Day')[['Rev$']].sum().reset_index(drop = True)
    dfsumrun['Avg1'] = dfsumrun.rolling(window=4).mean()
    dfsumrun = dfsumrun.assign(Avg1=np.where(dfsumrun.shape[0]<4, dfsumrun['Rev$'].mean(), dfsumrun.Avg1))
    runrate = round(dfsumrun['Avg1'].mean()*numofdays,2)
    dfsumrunlast = dfmain[condition2].groupby(['Day'])[['Rev$']].sum().reset_index(drop = True)
    dfsumrunlast['Avg1'] = dfsumrunlast.rolling(window=4).mean()
    dfsumrunlast = dfsumrunlast.assign(Avg1=np.where(dfsumrunlast.shape[0]<4, dfsumrunlast['Rev$'].mean(), dfsumrunlast.Avg1))
    runratelast = round(dfsumrunlast['Avg1'].mean()*lastnumofdays,2)
    return mtdsumthis, mtdsumlast, runrate, runratelast, dfmtd

def ytd(dfmain,today1,daysleft):
    dfyrsumrun = dfmain[(dfmain.Year == today1.year)&(dfmain.Date <= pd.to_datetime(today1))].groupby('Date')[['Rev$']].sum().reset_index(drop =False)
    ytdsum = round(dfyrsumrun['Rev$'].sum(),2)
    dfyrsumrun['Avg1'] = dfyrsumrun.rolling(window=4).mean()
    dfyrsumrun = dfyrsumrun.assign(Avg1=np.where(dfyrsumrun.shape[0]<4, dfyrsumrun['Rev$'].mean(), dfyrsumrun.Avg1))
    fyrunrate = round((dfyrsumrun['Avg1'].mean()*daysleft)+ytdsum,2)
    return ytdsum, fyrunrate

def get_table_download_link(df,name):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(
        csv.encode()
    ).decode()  # some strings <-> bytes conversions necessary here
    return f'<a href="data:file/csv;base64,{b64}" download="{name}.csv"> Download {name} </a>'

@st.cache(ttl=60)
def daily_product_notes(dfmain, today1, yesterday1, yesstr, yest, todaystr, today, metric, product_selected = ['Collections','Payouts','FX'], no_of_merch =5):
    dfdn = pd.DataFrame()
    for product in product_selected:
        dfmernoteThis = dfmain[(dfmain.Date == pd.to_datetime(today1))&(dfmain.Product3 == product)].groupby(['Product3','MerchName2','Currency'])[[metric]].sum().reset_index()
        dfmernoteThis = dfmernoteThis.sort_values(metric, ascending=[False])
        dfmernoteThis = dfmernoteThis.reset_index(drop = True)
        dfmernoteThis['MerchName2'] = dfmernoteThis['MerchName2'].replace(['Critical Ideas, Inc.'],'Chipper Cash App')
        dfmernoteThis = dfmernoteThis.groupby(['Product3','MerchName2','Currency'])[[metric]].sum().sort_values(metric, ascending = False)
        dfmernoteThis = dfmernoteThis.reset_index()
        dfmernoteLast = dfmain[(dfmain.Date == pd.to_datetime(yesterday1))&(dfmain.Product3 == product)].groupby(['Product3','MerchName2','Currency'])[[metric]].sum().reset_index()
        dfmernoteLast = dfmernoteLast.sort_values(metric, ascending=[False])
        dfmernoteLast = dfmernoteLast.reset_index(drop = True)
        dfmernoteLast['MerchName2'] = dfmernoteLast['MerchName2'].replace(['Critical Ideas, Inc.'],'Chipper Cash App')
        dfmernoteLast = dfmernoteLast.groupby(['Product3','MerchName2','Currency'])[[metric]].sum().sort_values(metric, ascending = False)
        dfmernoteLast = dfmernoteLast.reset_index()
        dfdailynote = dfmernoteLast.merge(dfmernoteThis, on=['Product3','MerchName2','Currency'], how='outer')
        dfdailynote.fillna(0, inplace = True)
        dfdailynote['Variance'] = dfdailynote[f'{metric}_y'] - dfdailynote[f'{metric}_x']
        dfdailynote = dfdailynote.sort_values('Variance', ascending=[False])
        dfdailynote = dfdailynote.reset_index(drop = True)
        dfdailynote['% of Rise/Drop'] = np.where(dfdailynote.Variance < 0,-dfdailynote.Variance/dfdailynote[(dfdailynote.Variance < 0)]['Variance'].sum(),dfdailynote.Variance*100/dfdailynote[dfdailynote.Variance > 0]['Variance'].sum())
        dfdailynote.rename(columns={f'{metric}_x': f'{yesstr}, {yest}', f'{metric}_y': f'{todaystr}, {today}'}, inplace=True)
        result = dfdailynote.iloc[(dfdailynote.index < no_of_merch) | (dfdailynote.index > len(dfdailynote)-no_of_merch-1)]
        dfdn = dfdn.append(result)
    dfdn = dfdn.reset_index(drop=True)
    dfdn.rename(columns = {'Product3': 'Product'},inplace=True)
    return dfdn

def projection(dfpro,numofdays,dfmain,today1):
    df = dfmain[(dfmain.Year == today1.year) & (dfmain.Month == today1.month)].groupby('MerchName2').agg(Actual = ('Rev$', sum)).sort_values(['Actual'], ascending = False).reset_index()
    df = df[df.index <= 1000]
    df['MerchName2'] = df['MerchName2'].replace(['Critical Ideas, Inc.'],'Chipper Cash App')
    df = df.groupby('MerchName2')[['Actual']].sum().sort_values('Actual', ascending = False)
    df = df.reset_index()
    df['Actual'] = df['Actual'].map(lambda x: round(x,2))
    dfpro['Proj MTD'] = dfpro.bestCase.map(lambda x: round(x*(today1.day)/numofdays,2))
    df1 = dfpro.merge(df, on='MerchName2', how='inner')
    df1['% Achieved'] = df1['Actual']/df1['Proj MTD'].map(lambda x: float(x))
    df1['% Achieved'] = df1['% Achieved'].map(lambda x: round(x,2))
    df1 = df1.sort_values('Proj MTD', ascending=False).reset_index(drop=True)
    return df1



### Weekly Report Functions

def week_summary(dfmain,today1,year,lastweekyear,thisweek,lastweek,thismonth,lastmonth,numofdays):
    #page1
    dfweek = dfmain[(dfmain.Week == thisweek)&(dfmain.Year == year)].groupby('Product3')[['Rev$','TPV$','TPC']].sum().reset_index()
    dflastweek = dfmain[(dfmain.Week == lastweek)&(dfmain.Year == lastweekyear)].groupby('Product3')[['Rev$','TPV$','TPC']].sum().reset_index()
    df1 = dfweek.loc[:,'Product3':'Rev$']
    df1['Product3'] = pd.Categorical(df1['Product3'], categories=['Collections', 'Payouts' ,'FX','Barter', 'Others'], ordered=True)
    df1.sort_values('Product3',inplace =True)
    df1 = df1.reset_index(drop = True)
    df2 = dflastweek.loc[:,'Product3':'Rev$']
    df2['Product3'] = pd.Categorical(df2['Product3'], categories=['Collections', 'Payouts','FX','Barter', 'Others'], ordered=True)
    df2.sort_values('Product3',inplace =True)
    df2 = df2.reset_index(drop = True)
    dfweeksum = df1.merge(df2, on='Product3')
    dfweeklyrev = dfmain[(dfmain.Year == year)].groupby('Week')[['Rev$']].sum().reset_index()
    dfweeklyrev.iloc[:,1] = dfweeklyrev.iloc[:,1].map(lambda x: round(x,2))
    #Week Summary -  Revenue, TPV, Revenue Increase and Run Rate
    weekrevsL = dflastweek['Rev$'].sum()
    weekrevs = dfweek['Rev$'].sum()
    weekTPVsL = dflastweek['TPV$'].sum()
    weekTPVs = dfweek['TPV$'].sum()
    weekTPCL = dflastweek['TPC'].sum()
    weekTPC = dfweek['TPC'].sum()
    #Week Summary - Target Achieved
    totmonthrev = dfmain[(dfmain.Month == thismonth)&(dfmain.Year == year)]['Rev$'].sum()
    #Week Summary - runrate
    dfsumrun = dfmain[(dfmain.Month == thismonth)&(dfmain.Year == year)].groupby('Day')[['Rev$']].sum().reset_index(drop =True)
    dfsumrun['Avg1'] = dfsumrun.rolling(window=4).mean()
    dfsumrun = dfsumrun.assign(Avg1=np.where(dfsumrun.shape[0]<4, dfsumrun['Rev$'].mean(), dfsumrun.Avg1))
    monrunrate = dfsumrun['Avg1'].mean()*numofdays
    weekStat = [weekrevs,weekTPVs,weekTPC,totmonthrev,monrunrate]
    weekStat2 = [weekrevsL,weekTPVsL,weekTPCL]
    return df1, df2, dfweek, dflastweek, dfweeksum, weekStat, dfweeklyrev, dfweeksum, weekStat2

def week_exfx_summary(dfmain,year,dfweek,dflastweek,thisweek,thismonth,numofdays):
    #Ex FX
    dfweeklyrevexFX = dfmain[(dfmain.Year == year)&(dfmain.Product3 != 'FX')].groupby('Week')[['Rev$']].sum().reset_index()
    weekrevFX = dfweek[dfweek['Product3'] != 'FX']['Rev$'].sum()
    weekrevFXL = dflastweek[dflastweek['Product3'] != 'FX']['Rev$'].sum()
    weekTPVFX = dfweek[dfweek['Product3'] != 'FX']['TPV$'].sum()
    weekTPVFXL = dflastweek[dflastweek['Product3'] != 'FX']['TPV$'].sum()
    weekTPCFX = dfweek[dfweek['Product3'] != 'FX']['TPC'].sum()
    weekTPCFXL = dflastweek[dflastweek['Product3'] != 'FX']['TPC'].sum()
    #Week exFX Summary - Target Achieved
    totmonthrevexFX = dfmain[(dfmain.Year == year)& (dfmain.Month == thismonth) & (dfmain.Product3 != 'FX')]['Rev$'].sum()
    #Week exFX Summary - runrate
    dfsumrunexFX1 = dfmain[(dfmain.Year == year) & (dfmain.Month == thismonth) & (dfmain.Product3 != 'FX')].groupby('Day')[['Rev$']].sum().reset_index(drop =True)
    dfsumrunexFX1['Avg1'] = dfsumrunexFX1.rolling(window=4).mean()
    monrunrateexFX = dfsumrunexFX1['Rev$'].mean()*numofdays
    weekStatexFX = [weekrevFX,weekTPVFX,weekTPCFX,totmonthrevexFX,monrunrateexFX]
    weekStatexFX2 = [weekrevFXL,weekTPVFXL,weekTPCFXL]
    return dfweeklyrevexFX, weekStatexFX,weekStatexFX2

def week_colpay_summary(product,year,dfmain,dfweek,dflastweek,thisweek):
    #Collections Performance
    dfweekColpay = dfmain[(dfmain.Year == year)&(dfmain.Product3 == product)].groupby('Week')[['Rev$','TPV$','TPC']].sum().reset_index()
    sumrevColpay = dfweek[dfweek.Product3 == product]['Rev$'].item()
    sumrevColpayL = dflastweek[dflastweek.Product3 == product]['Rev$'].item()
    sumTPVColpay = dfweek[dfweek.Product3 == product]['TPV$'].item()
    sumTPVColpayL = dflastweek[dflastweek.Product3 == product]['TPV$'].item()
    sumTPCColpay = dfweek[dfweek.Product3 == product]['TPC'].item()
    sumTPCColpayL = dflastweek[dflastweek.Product3 == product]['TPC'].item()
    weekStatColpay = [sumrevColpay,sumTPVColpay,sumTPCColpay]
    weekStatColpay2 = [sumrevColpayL,sumTPVColpayL,sumTPCColpayL]
    return dfweekColpay, weekStatColpay, weekStatColpay2

def week_barter_performance(dfmain,lastweekyear,year,thisweek,lastweek,dfweek,dflastweek):
    sumrevBar = dfweek[dfweek.Product3 == 'Barter']['Rev$'].item()
    sumrevBarL = dflastweek[dflastweek.Product3 == 'Barter']['Rev$'].item()
    sumTPVBar = dfweek[dfweek.Product3 == 'Barter']['TPV$'].item()
    sumTPVBarL = dflastweek[dflastweek.Product3 == 'Barter']['TPV$'].item()
    sumTPCBar = dfweek[dfweek.Product3 == 'Barter']['TPC'].item()
    sumTPCBarL = dflastweek[dflastweek.Product3 == 'Barter']['TPC'].item()
    weekStatBar = [sumrevBar,sumTPVBar,sumTPCBar]
    weekStatBar2 = [sumrevBarL,sumTPVBarL,sumTPCBarL]
    dfBarter = dfmain[(dfmain.Year == year)&(dfmain.Week == thisweek)&(dfmain.Product3 == 'Barter')].groupby('Product')[['Rev$','TPV$','TPC']].sum().reset_index()
    dfBarter = dfBarter[dfBarter['Product'].isin(['Airtime & bills', 'Barter Send Money to Bank','Card Transactions','Mvisa Qr Payment', 'Card Issuance Fee', 'Pay with Barter','Send Money','Wallet Funding'])].reset_index(drop = True)
    dfBarterLast = dfmain[(dfmain.Year == lastweekyear)&(dfmain.Week == lastweek)&(dfmain.Product3 == 'Barter')].groupby('Product')[['Rev$','TPV$','TPC']].sum().reset_index()
    dfBarterLast = dfBarterLast[dfBarterLast['Product'].isin(['Airtime & bills', 'Barter Send Money to Bank','Card Transactions','Card Issuance Fee','Mvisa Qr Payment', 'Pay with Barter','Send Money','Wallet Funding'])].reset_index(drop = True)
    dfB = dfBarterLast.merge(dfBarter, on='Product', how='inner')
    dfB['Rev$ Variance'] = dfB['Rev$_y'] - dfB['Rev$_x'] 
    #dfB['TPV$ Variance'] = dfB['TPV$_y'] - dfB['TPV$_x']
    dfB['Rev$ Variance %'] = np.where(dfB['Rev$ Variance'] < 0,dfB['Rev$ Variance']*100/dfB[dfB['Rev$ Variance'] < 0]['Rev$ Variance'].sum(),dfB['Rev$ Variance']*100/dfB[dfB['Rev$ Variance'] > 0]['Rev$ Variance'].sum())
    #dfB['TPV$ Variance %'] = np.where(dfB['TPV$ Variance'] < 0,dfB['TPV$ Variance']*100/dfB[dfB['TPV$ Variance'] < 0]['TPV$ Variance'].sum(),dfB['TPV$ Variance']*100/dfB[dfB['TPV$ Variance'] > 0]['TPV$ Variance'].sum())
    dfB.rename(columns={'Rev$_x': f'{lastweek} Revenue', 'TPV$_x': f'{lastweek} TPV', 'TPC_x': f'{lastweek} TPC','Rev$_y': f'{thisweek} Revenue', 'TPV$_y': f'{thisweek} TPV', 'TPC_y': f'{thisweek} TPC'}, inplace=True)
    dfweekrevBar = dfmain[(dfmain.Year == year)&(dfmain.Product3 == 'Barter')].groupby('Week')[['Rev$']].sum().reset_index()
    dfweektpvBar = dfmain[(dfmain.Year == year)&(dfmain.Product3 == 'Barter')].groupby(['Product','Week'])[['TPV$']].sum().reset_index()
    dfweektpvBar = dfweektpvBar[dfweektpvBar.Product.isin(['Airtime & bills', 'Barter Send Money to Bank','Card Transactions','Mvisa Qr Payment', 'Card Issuance Fee', 'Pay with Barter','Send Money','Wallet Funding'])]
    return dfB, dfweekrevBar, dfweektpvBar, weekStatBar, weekStatBar2

def pos_agency(dfmain,lastweekyear,thisweek,lastweek,year):
    dfagency1 = dfmain[(dfmain.Year == year)&(dfmain.Product3 != 'FX')&((dfmain.Vertical == 'Agency')|(dfmain.Vertical == 'POS'))].groupby('Week')[['Fees','Rev$','TPV$','TPV','TPC']].sum().reset_index()
    dfagency = dfmain[(dfmain.Week.isin([thisweek,lastweek]))&(dfmain.Product3 != 'FX')&((dfmain.Vertical == 'Agency')|(dfmain.Vertical == 'POS'))].groupby('Week')[['Fees','Rev$','TPV$','TPV','TPC']].sum().reset_index()
    totrevAgency = dfagency[(dfmain.Year == year)&(dfagency.Week == thisweek)]['Rev$'].item()
    totrevAgencyL = dfagency[(dfmain.Year == lastweekyear)&(dfagency.Week == lastweek)]['Rev$'].item()
    tottpvAgency = dfagency[(dfmain.Year == year)&(dfagency.Week == thisweek)]['TPV$'].item()
    tottpvAgencyL = dfagency[(dfmain.Year == lastweekyear)&(dfagency.Week == lastweek)]['TPV$'].item()
    tottpcAgency = dfagency[(dfmain.Year == year)&(dfagency.Week == thisweek)]['TPC'].item()
    tottpcAgencyL = dfagency[(dfmain.Year == lastweekyear)&(dfagency.Week == lastweek)]['TPC'].item()
    weekagencyStat = [totrevAgency,tottpvAgency,tottpcAgency]
    weekagencyStat2 = [totrevAgencyL,tottpvAgencyL,tottpcAgencyL]
    return dfagency1,weekagencyStat,weekagencyStat2


def currency_performance(dfmain,lastweekyear,thisweek,lastweek,year):
    dfcurThisF = dfmain[(dfmain.Year == year)&(dfmain.Week == thisweek)].groupby('Currency')[['Fees']].sum().reset_index()
    dfcurLastF = dfmain[(dfmain.Year == lastweekyear)&(dfmain.Week == lastweek)].groupby('Currency')[['Fees']].sum().reset_index()
    dfcurBothF = dfcurLastF.merge(dfcurThisF, on='Currency', how='outer')
    dfcurBothF['Variance'] = round((dfcurBothF['Fees_y'] - dfcurBothF['Fees_x'])/dfcurBothF['Fees_x'])
    dfcurBothF.rename(columns={'Fees_y':f'Week {thisweek} Revenue','Fees_x':f'Week {lastweek} Revenue'},inplace=True)
    dfcurBothF = dfcurBothF[dfcurBothF['Currency'].isin(['USD','NGN','KES','EUR', 'GHS', 'ZAR', 'UGX', 'GBP', 'XAF', 'AUD'])]
    dfcurBothF['Currency'] = pd.Categorical(dfcurBothF['Currency'], categories=['USD','NGN','KES','EUR', 'GHS', 'ZAR', 'UGX', 'GBP', 'XAF', 'AUD'], ordered=True)
    dfcurBothF.sort_values('Currency',inplace =True)
    dfcurBothF = dfcurBothF.reset_index(drop=True)
    dfcurBothF['Currency'] = dfcurBothF['Currency'].astype('str')
    dfrevCur = dfmain[dfmain['Currency'].isin(['USD','NGN','KES','EUR', 'GHS', 'ZAR', 'UGX', 'GBP', 'XAF', 'AUD'])].groupby(['Currency','Week'])[['Rev$']].sum().reset_index()
    return dfcurBothF, dfrevCur

def currency_note(dfmain,year,lastweekyear,thisweek,lastweek,currency_selected):
    if currency_selected:
        dfcurnot = dfmain[((dfmain.Year == lastweekyear)|(dfmain.year == year))&((dfmain.Week == thisweek)|(dfmain.Week== lastweek))&(dfmain['Currency'].isin(currency_selected))].groupby(['Year','Currency','Product3','Week','MerchName2'])[['Rev$']].sum().reset_index()
        dfcurnot.sort_values('Rev$', ascending = False, inplace=True)
        dfcurnot = dfcurnot.reset_index(drop=True)
        return dfcurnot
    else:
        pass



def cohort_analysis(dfmain,year,lastweekyear,thisweek, lastweek):
    dfweek = dfmain[(((dfmain.Year == year)|(dfmain.Year == lastweekyear))&((dfmain.Week == thisweek)|(dfmain.Week == lastweek)))].groupby('Product3')[['Rev$','TPV$','TPC']].sum().reset_index()
    dfmerThis = dfmain[(dfmain.Year == year)&(dfmain.Week == thisweek)].groupby('MerchName2')[['Rev$']].sum().reset_index()
    dfmerThis = dfmerThis.sort_values('Rev$', ascending=[False])
    dfmerThis = dfmerThis[~dfmerThis.MerchName2.isin(['(Blank)','Barter','RAVE-PAYOUT'])]
    dfmerThis = dfmerThis.reset_index(drop = True)
    dfmerLast = dfmain[(dfmain.Year == lastweekyear)&(dfmain.Week == lastweek)].groupby('MerchName2')[['Rev$']].sum().reset_index()
    dfmerLast = dfmerLast.sort_values('Rev$', ascending=[False])
    dfmerLast = dfmerLast.reset_index(drop = True)
    dfcoh = dfmerThis.merge(dfmerLast, on='MerchName2', how='outer').fillna(0)
    dfcoh['MerchName2'] = dfcoh['MerchName2'].replace(['Critical Ideas, Inc.'],'Chipper Cash App')
    dfcoh = dfcoh.groupby('MerchName2').sum().sort_values('Rev$_x', ascending = False)
    dfcoh['Variance'] = round((dfcoh['Rev$_x'] - dfcoh['Rev$_y'])/dfcoh['Rev$_y'], 2)
    dfcoh.rename(columns={'Rev$_x':f'Week {thisweek} Revenue','Rev$_y':f'Week {lastweek} Revenue'},inplace=True)
    dfcoh = dfcoh.reset_index()
    df50 = dfmerThis.iloc[0:51,:]
    df50['MerchName2'] = df50['MerchName2'].replace(['Critical Ideas, Inc.'],'Chipper Cash App')
    df50 = df50.groupby('MerchName2')[['Rev$']].sum().sort_values('Rev$', ascending = False)
    df50 = df50.reset_index()
    summercRev = dfweek['Rev$'].sum()
    sumdf50merc = df50['Rev$'].sum()
    df20 = df50.iloc[0:20,:]
    sumdf20merc = df20['Rev$'].sum()
    df10 = df50.iloc[0:10,:]
    sumdf10merc = df10['Rev$'].sum()
    cohanalStat = [sumdf50merc,sumdf20merc,sumdf10merc,summercRev]
    return dfcoh,cohanalStat


def weekly_new_old_merch(merlist,year,dfmain):
    dfNew1 = dfmain[(dfmain.Year==year)&(dfmain.MerchName2.isin(merlist))]
    dfNewp = pd.pivot_table(dfNew1, values = 'Rev$', index=['MerchName2'], columns = 'Week', aggfunc = np.sum).reset_index()
    dfNewp1 = pd.DataFrame(dfNewp.to_records())
    dfNewp1.drop(dfNewp1.columns[0], axis = 1,inplace=True)
    dfNewp1.fillna(0,inplace = True)
    cols = dfNewp1.columns
    dfNewp = dfNewp1[['MerchName2']]
    dfNewp[cols[1]] = dfNewp1[cols[1]]
    for i in range(2,len(cols[1:])):
        dfNewp[cols[i+1]] = dfNewp1[cols[i]]-dfNewp1[cols[i-1]]
    dfNewp.fillna(0,inplace=True)
    col2dis = [cols[0],*cols[-10:]]
    return dfNewp[col2dis]






### Team Function
@st.cache(allow_output_mutation=True)
def team_rev(dfmain, year, team_name, team_month, team_quar, team_class, team_cat,team_parameter,team_merch):
    if 'All' in team_name:
        if 'All' in team_merch:
            dfteamrev1 = dfmain[(dfmain.Year == year)&(dfmain.Classification.isin(team_class))&(dfmain.Category.isin(team_cat))&(dfmain.Month.isin(list(range(team_month[0],team_month[1]+1,1))))&(dfmain.Quarter.isin(list(range(team_quar[0],team_quar[1]+1,1))))].groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(by='Rev$', ascending=False).reset_index()
        elif 'All' not in team_merch:
            dfteamrev1 = dfmain[(dfmain.Year == year)&(dfmain.MerchName2.isin(team_merch))&(dfmain.Classification.isin(team_class))&(dfmain.Category.isin(team_cat))&(dfmain.Month.isin(list(range(team_month[0],team_month[1]+1,1))))&(dfmain.Quarter.isin(list(range(team_quar[0],team_quar[1]+1,1))))].groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(by='Rev$', ascending=False).reset_index()

    elif 'All' not in team_name:
        if 'All' in team_merch:
            dfteamrev1 = dfmain[(dfmain.Year == year)&(dfmain.Vertical.isin(team_name))&(dfmain.Classification.isin(team_class))&(dfmain.Category.isin(team_cat))&(dfmain.Month.isin(list(range(team_month[0],team_month[1]+1,1))))&(dfmain.Quarter.isin(list(range(team_quar[0],team_quar[1]+1,1))))].groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(by='Rev$', ascending=False).reset_index()
        elif 'All' not in team_merch:
            dfteamrev1 = dfmain[(dfmain.Year == year)&(dfmain.Vertical.isin(team_name))&(dfmain.MerchName2.isin(team_merch))&(dfmain.Classification.isin(team_class))&(dfmain.Category.isin(team_cat))&(dfmain.Month.isin(list(range(team_month[0],team_month[1]+1,1))))&(dfmain.Quarter.isin(list(range(team_quar[0],team_quar[1]+1,1))))].groupby(team_parameter)[["Rev$"]].sum()
            dfteamrev1 = dfteamrev1.sort_values(by='Rev$', ascending=False).reset_index()
    else:
        st.warning('wrong input')
    return dfteamrev1


#budget performance

@st.cache
def get_pipeline(sheetnames=['BUSINESS DEV_OPEYEMI FOWLER','IMTO_BASSEY','TRAVEL & HOSPITALITY_YEWANDE','GAMING_WALE','GHANA',
                  'KENYA','UGANDA','RWANDA','ZAMBIA','SK','EUROPE'],sheetcols=['S/N','PROSPECT','SECTOR','OFFERING','PROJECTED ANNUAL REVENUE ($)','ESTIMATED MONTHLY REVS ($)','ESTIMATED WEEKLY REVENUE ($)','ENTERPRISE LEVEL','COMMENCE TRANSACTING','REVENUE WEEKS AVAILABLE','STAGE']):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SPREADSHEET_ID = '1WUf3vNSrRD6OIRsMCgMs1mENQYfKNH42e2AwCARGvL0'
    dfpip = pd.DataFrame()
    for RANGE_NAME in sheetnames:
        data = pull_sheet_data(SCOPES,SPREADSHEET_ID,RANGE_NAME)
        df = pd.DataFrame(data[1:], columns=sheetcols)
        df['SHEET'] = RANGE_NAME
        dfpip = pd.concat([dfpip,df.reset_index(drop=True)],axis=0,ignore_index=True)
    return dfpip

def process_pipeline(dfpip,team_name):
    dfpip = dfpip.reset_index(drop=True)
    col         = 'SHEET'
    conditions  = [ dfpip[col] == 'IMTO_BASSEY',
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
    choices     = ['IMTO','Betting/Gaming', 'Ghana', 'Kenya', 'Uganda','Rwanda','Zambia','Sekinat','Europe', 'BusinessDev', 'PSP' ]
    dfpip["Vertical"] = np.select(conditions, choices, default=np.nan)
    dfpip.dropna(inplace=True)
    dfpip = dfpip.reset_index(drop=True)
    if 'All' not in team_name:
        dfpip = dfpip[dfpip.Vertical.isin(team_name)]
    else:
        dfpip = dfpip
    dfpip['PROJECTED ANNUAL REVENUE ($)'] = dfpip['PROJECTED ANNUAL REVENUE ($)'].map(lambda x: x.replace(',','')).str.extract('(\d+)')
    dfpip['ESTIMATED MONTHLY REVS ($)'] = dfpip['ESTIMATED MONTHLY REVS ($)'].map(lambda x: x.replace(',','')).str.extract('(\d+)')
    dfpip['ESTIMATED WEEKLY REVENUE ($)'] = dfpip['ESTIMATED WEEKLY REVENUE ($)'].map(lambda x: x.replace(',','')).str.extract('(\d+)')
    dfpip['PROJECTED ANNUAL REVENUE ($)'] = dfpip['PROJECTED ANNUAL REVENUE ($)'].astype(float)
    dfpip['ESTIMATED MONTHLY REVS ($)'] = dfpip['ESTIMATED MONTHLY REVS ($)'].astype(float)
    dfpip['ESTIMATED WEEKLY REVENUE ($)'] = dfpip['ESTIMATED WEEKLY REVENUE ($)'].astype(float)
    totexprev = dfpip['PROJECTED ANNUAL REVENUE ($)'].sum()
    numoflive = dfpip[dfpip.STAGE=='Live']['PROSPECT'].nunique()
    expfromlive = dfpip[dfpip.STAGE=='Live']['PROJECTED ANNUAL REVENUE ($)'].sum()
    #livetarach = round(expfromlive*100/totexprev)
    dfpros = dfpip.groupby(['PROSPECT'])[['ESTIMATED MONTHLY REVS ($)','PROJECTED ANNUAL REVENUE ($)','ESTIMATED WEEKLY REVENUE ($)']].sum().reset_index()
    dfpros = dfpros.sort_values('ESTIMATED MONTHLY REVS ($)', ascending=False).reset_index(drop=True)
    dfstage = dfpip.groupby(['STAGE'])[['PROSPECT']].count().reset_index()
    dfstage = dfstage[dfstage.STAGE.isin(['Leads generation','Qualify Leads','Initiate contact & Identify Needs','Agreement','Solutions Presentation', 'Integration', 'Negotiation','Pilot','Live'])]
    dfstage['STAGE'] = pd.Categorical(dfstage['STAGE'], categories=['Leads generation','Qualify Leads','Initiate contact & Identify Needs','Agreement','Solutions Presentation', 'Integration', 'Negotiation','Pilot','Live'], ordered=True)
    dfstage.sort_values('STAGE',inplace =True)
    dfstage['Variance'] = dfstage.PROSPECT.map(lambda x: round(x*100/dfstage.PROSPECT.sum()))
    dfstage = dfstage.reset_index(drop=True)
    pipeStat = [totexprev,expfromlive]
    
    return pipeStat, numoflive, dfpros, dfstage


# Account Management Report Functions

def gainers_losers(dfmain,year,thismonth,team_name):
    if 'All' in team_name:
        dfxx = pd.pivot_table(dfmain,
                index='MerchName2',
                values='Rev$',
                columns=['Year','Month'],
                fill_value=0,
                aggfunc=np.sum,
                )
    else:
        dfxx = pd.pivot_table(dfmain[dfmain.Vertical.isin(team_name)],
                index='MerchName2',
                values='Rev$',
                columns=['Year','Month'],
                fill_value=0,
                aggfunc=np.sum,
                )
    dfxx = dfxx.sort_values((year,thismonth),ascending=False).reset_index()
    dfxx.rename(columns={'MerchName2':'Merchants'},inplace=True)
    j = 3
    for i in range(0,len(dfxx.columns)+1,4):
        try:
            dfxx.insert(j+1,f'{year} Quarter {(j+1)//4} Total',dfxx.iloc[:,i+1:j+1].sum(axis=1))
            j+=4
        except:
            pass
    try:
        col2dis = dfxx.columns.tolist()
        col2dis[:] = col2dis[0:1]+col2dis[-8:]
        dfxx = dfxx[col2dis]
    except:
        pass
    for i in range(len(dfxx.columns)):
        if len(dfxx.columns[-i][0])>15:
            colname = dfxx.columns[-i][0]
            colname2 = dfxx.columns[-i-4][0]
            break
    try:
        dfxx['Variance'] = dfxx[colname] - dfxx[colname2]
    except:
        dfxx['Variance'] = dfxx[colname]

    dfxxgain = dfxx[dfxx['Variance']>0].sort_values('Variance',ascending=False).reset_index(drop=True)      
    dfxxloss = dfxx[dfxx['Variance']<0].sort_values('Variance',ascending=True).reset_index(drop=True) 
    
    return dfxxgain,dfxxloss


