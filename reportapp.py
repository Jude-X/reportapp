import pandas as pd
import psycopg2
import pandas.io.sql as psql
import SessionState
import base64
import streamlit as st
import streamlit.components.v1 as components
import datetime
import numpy as np
import calendar
from utils import team_rev, yesterday_dates, week_dates, month_dates, year_dates, conditions, df_today, color_change, pro_color_change, df_sum, mtd, ytd, get_table_download_link, daily_product_notes, week_summary, week_exfx_summary, week_colpay_summary, week_barter_performance, pos_agency, currency_performance, currency_note, weekly_new_old_merch, cohort_analysis, get_pipeline, process_pipeline, projection,gainers_losers
from graphs import daily_report_graphs, weekly_report_graphs, vertical_budget_graphs, pipeline_tracker_graphs, card_indicators,card_indicators2,table_fig,bar_indicator
from calendar import monthrange
from PIL import Image
import plotly.graph_objects as go
from plotly import tools
import plotly.offline as py
import plotly.express as px
import os
from plotly.subplots import make_subplots
from dotenv import load_dotenv
from urllib.parse import urlparse # for python 3+ use: from urllib.parse import urlparse

load_dotenv()
img = Image.open('flutterwavelogo.jpg')

st.set_page_config(page_title='Flutterwave Dashboard',layout='wide')


result = urlparse(os.getenv("DATABASE_URL"))
# also in python 3+ use: urlparse("YourUrl") not urlparse.urlparse("YourUrl") 
username = result.username
password = result.password
database = result.path[1:]
hostname = result.hostname

# Connect to the PostgreSQL database server
conn = psycopg2.connect(
    database = database,
    user = username,
    password = password,
    host = hostname
)

conn.autocommit = True
c = conn.cursor()


# \COPY datatable(TPV, TPC, Fees, Month, Date, Currency, Merchants, country, Product, Product2, Product3, TPV$, Rev$, Day, Product4, Week, Vertical, Global, Product_FX, New_Ex, MerchName2, Category, Classification, Quarter, DayOfWeek, DayName, Year) FROM 'C:\Users\Nzubechukwu Onyekaba\Desktop\project\data.csv' DELIMITER ',' CSV HEADER encoding 'UTF8';
#get data function
@st.cache(hash_funcs={psycopg2.extensions.connection: id},ttl=3600,max_entries=1)
def get_data(conn):
    '''
        This function gets data from a csv file. Once the data warehouse project is complete,
        itll be configured to take data from database
    '''
    #dfmain = psql.read_sql('SELECT * FROM datatable',conn,parse_dates='date')
    dfmain = pd.read_csv('data.csv',parse_dates=['Date'])
    cols = dfmain.columns.tolist()
    for i in range(len(cols)):
        if cols[i] in ['tpv','tpc','tpv$','id']:
            cols[i] = cols[i].upper()
            
        elif cols[i] in ['product_fx', 'new_ex', 'merchname2']:
            if cols[i] == 'product_fx':
                cols[i] = 'Product/FX'
            elif cols[i] == 'New_ex':
                cols[i] = 'New_Ex'
            elif cols[i] == 'merchname2':
                cols[i] = 'MerchName2'
        
        elif cols[i] == 'country':
            pass
        
        else:
            cols[i] = cols[i].title()

    #dfmain.columns = cols

    return dfmain
# Database Functions
def data_table():
    c.execute(
    '''
    CREATE TABLE IF NOT EXISTS datatable(
	ID BIGSERIAL PRIMARY KEY,
	TPV DECIMAL(13,2) DEFAULT 0,
    TPC BIGINT DEFAULT 1 CHECK (TPC>=0),
    Fees DECIMAL(13,2) DEFAULT 0,
	Month SMALLINT DEFAULT NULL CHECK (Month<=12),
	Date TIMESTAMP DEFAULT NULL,
	Currency VARCHAR(4) DEFAULT NULL,
	Merchants VARCHAR(250) DEFAULT NULL,
	country VARCHAR(3) DEFAULT NULL,
	Product VARCHAR(100) DEFAULT NULL,
	Product2 VARCHAR(75) DEFAULT NULL,
	Product3 VARCHAR(25) DEFAULT NULL,
	TPV$ DECIMAL(13,2) DEFAULT 0,
	Rev$ DECIMAL(11,2) DEFAULT 0,
	Day SMALLINT DEFAULT NULL CHECK (Day<=31),
	Product4 VARCHAR(15) DEFAULT NULL,
	Week SMALLINT DEFAULT NULL CHECK (Week<=53),
	Vertical VARCHAR(25) DEFAULT NULL,
	Global VARCHAR(25) DEFAULT NULL,
	Product_FX VARCHAR(15) DEFAULT NULL,
	New_Ex VARCHAR(18) DEFAULT NULL,
	MerchName2 VARCHAR(250) DEFAULT NULL,
	Category VARCHAR(15) DEFAULT NULL,
	Classification VARCHAR(10) DEFAULT NULL,
	Quarter SMALLINT DEFAULT NULL CHECK (Quarter<=4),
	DayOfWeek SMALLINT DEFAULT NULL CHECK (DayOfWeek<=9),
	DayName VARCHAR(5) DEFAULT NULL,
	Year SMALLINT DEFAULT NULL CHECK (Year>=2016)
    )
    ''')


def create_usertable():
	c.execute('CREATE TABLE IF NOT EXISTS userstable(id SERIAL PRIMARY KEY, email VARCHAR(50) UNIQUE, vertical VARCHAR(25), password VARCHAR(25))')


def add_userdata(email,vertical,password):
	c.execute('INSERT INTO userstable(email,vertical,password) VALUES (%s,%s,%s)',(email,vertical,password))


def login_user(email,password):
	c.execute('SELECT * FROM userstable WHERE email = %s AND password = %s',(email,password))
	data = c.fetchall()
	return data

def create_targetable():
    c.execute('CREATE TABLE IF NOT EXISTS targetable(id SERIAL PRIMARY KEY, last_month_target INTEGER, month_target INTEGER, year_target INTEGER)')


def view_all_users():
    c.execute('SELECT * FROM userstable')
    data = c.fetchall()
    return data

def view_all_targets():
    c.execute('SELECT * FROM vertargetable')
    data = c.fetchall()
    return data

def update_target(lastmonthtarget=0,monthtarget=0,yeartarget=0):
    if lastmonthtarget != 0:
        c.execute('UPDATE targetable SET last_month_target = %s WHERE id = %s',(lastmonthtarget,1))
    if monthtarget != 0:
        c.execute('UPDATE targetable SET month_target = %s WHERE id = %s',(monthtarget,1))
    if yeartarget != 0:
        c.execute('UPDATE targetable SET year_target = %s WHERE id = %s',(yeartarget,1))

def get_target():
    c.execute('SELECT * FROM targetable WHERE id = 1')
    data = c.fetchall()
    return data

def view_target():
    c.execute('SELECT * FROM targetable')
    data = c.fetchall()
    return data

def create_notes():
    c.execute('''CREATE TABLE IF NOT EXISTS dailysumnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, dailysum VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS weeklysumnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, weeklysumn VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS weeklycurrnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, weeklycurr VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS weeklybarnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, weeklybar VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS accmgtgainnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, accmgtgain VARCHAR(3000))''')
    c.execute('''CREATE TABLE IF NOT EXISTS accmgtlossnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, accmgtloss VARCHAR(3000))''')
    c.execute('''CREATE TABLE IF NOT EXISTS smesummnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, smesumm VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS pipelinenotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, pipeline VARCHAR(1500))''')
    
    


def edit_notes(today1,note,nameofnote):
    if note:
        if datetime.datetime.now().day - today1.day >= 0:
            if nameofnote == 'DailySummary':
                try:
                    c.execute('INSERT INTO dailysumnotes(date_created,dailysum) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE dailysumnotes SET dailysum = %s WHERE date_created = %s',(note,today1))
            elif nameofnote == 'WeeklySummary':
                try:
                    c.execute('INSERT INTO weeklysumnotes(date_created,weeklysum) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE weeklysumnotes SET weeklysum = %s WHERE date_created = %s',(note,today1))
            elif nameofnote == 'WeeklyCurrency':
                try:
                    c.execute('INSERT INTO weeklycurrnotes(date_created,weeklycurr) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE weeklycurrnotes SET weeklycurr = %s WHERE date_created = %s',(note,today1))
            elif nameofnote == 'WeeklyBarter':
                try:
                    c.execute('INSERT INTO weeklybarnotes(date_created,weeklybar) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE weeklybarnotes SET weeklybar = %s WHERE date_created = %s',(note,today1))
            elif nameofnote == 'AccMgtGain':
                try:
                    c.execute('INSERT INTO accmgtgainnotes(date_created,accmgtgain) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE accmgtgainnotes SET accmgtgain = %s WHERE date_created = %s',(note,today1))
            elif nameofnote == 'AccMgtLoss':
                try:
                    c.execute('INSERT INTO accmgtlossnotes(date_created,accmgtloss) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE accmgtlossnotes SET accmgtloss = %s WHERE date_created = %s',(note,today1))
            elif nameofnote == 'SME':
                try:
                    c.execute('INSERT INTO smesummnotes(date_created,smesumm) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE smesummnotes SET smesumm = %s WHERE date_created = %s',(note,today1))
            elif nameofnote == 'Pipeline':
                try:
                    c.execute('INSERT INTO pipelinenotes(date_created,pipeline) VALUES (%s,%s)',(today1,note))
                except Exception:
                    c.execute('UPDATE pipelinenotes SET pipeline = %s WHERE date_created = %s',(note,today1))

def view_notes(today1,nameofnote):
    if nameofnote == 'DailySummary':
        c.execute('SELECT * FROM dailysumnotes WHERE date_created = %s',([today1]))
    elif nameofnote == 'WeeklySummary':
        c.execute('SELECT * FROM weeklysumnotes WHERE date_created = %s',([today1]))
    elif nameofnote == 'WeeklyCurrency':
        c.execute('SELECT * FROM weeklycurrnotes WHERE date_created = %s',([today1]))
    elif nameofnote == 'WeeklyBarter':
        c.execute('SELECT * FROM weeklybarnotes WHERE date_created = %s',([today1]))
    elif nameofnote == 'AccMgtGain':
        c.execute('SELECT * FROM accmgtgainnotes WHERE date_created = %s',([today1]))
    elif nameofnote == 'AccMgtLoss':
        c.execute('SELECT * FROM accmgtlossnotes WHERE date_created = %s',([today1]))
    elif nameofnote == 'SME':
        c.execute('SELECT * FROM smesummnotes WHERE date_created = %s',([today1]))
    elif nameofnote == 'Pipeline':
        c.execute('SELECT * FROM pipelinenotes WHERE date_created = %s',([today1]))
    data = c.fetchall()
    return data

def create_vertargetable():
    c.execute('CREATE TABLE IF NOT EXISTS vertargetable(id SERIAL PRIMARY KEY, vertical VARCHAR(55) UNIQUE, year_target INTEGER)')

def create_livetargetable():
    c.execute('CREATE TABLE IF NOT EXISTS livetargetable(id SERIAL PRIMARY KEY, vertical VARCHAR(55) UNIQUE, live_target INTEGER)')

def get_vertarget(team_name):
    c.execute('SELECT * FROM vertargetable WHERE vertical = %s',(team_name))
    data = c.fetchall()
    return data

def get_livetarget(team_name):
    c.execute('SELECT * FROM livetargetable WHERE vertical = %s',(team_name))
    data = c.fetchall()
    return data

def edit_vertargetable(team_name, target2): 
    try:
        c.execute('INSERT INTO vertargetable(vertical,year_target) VALUES (%s,%s)',(team_name[0],yeartarget2))
    except Exception:
        c.execute('UPDATE vertargetable SET year_target = %s WHERE vertical = %s',(yeartarget2,team_name[0]))

def edit_livetargetable(team_name, livetarget2): 
    try:
        c.execute('INSERT INTO livetargetable(vertical,live_target) VALUES (%s,%s)',(team_name[0],livetarget2))
    except Exception:
        c.execute('UPDATE livetargetable SET live_target = %s WHERE vertical = %s',(livetarget2,team_name[0]))

def create_bestcase():
    c.execute('CREATE TABLE IF NOT EXISTS projection(id SERIAL PRIMARY KEY, MerchName2 VARCHAR(75) UNIQUE, best_fig DECIMAL(9,2))')

def update_bestcase(merch_name, best_fig):
    if 'All' not in merch_name and best_fig !=1:
        try:
            c.execute('INSERT INTO projection(MerchName2,best_fig) VALUES (%s,%s)',(merch_name[0],best_fig))
        except Exception:
            c.execute('UPDATE projection SET best_fig = %s WHERE MerchName2 = %s',(best_fig,merch_name[0]))
    else:
        st.warning('Please input merchants one at a time, and unselect the All option')

def delete_bestcase(del_merch_name):
    if del_merch_name:
        try:
            for name in del_merch_name:
                c.execute('DELETE FROM projection WHERE MerchName2 = %s',([name]))
        except Exception:
            st.warning(f'{del_merch_name} Failed to delete Merchant, please try again')
        else:
            st.success(f'{del_merch_name} deleted sucessfully')
    else:
        pass

def delete_user(del_email):
    if del_email:
        try:
            for email in del_email:
                c.execute('DELETE FROM projection WHERE email = %s',([email]))
        except Exception:
            st.warning(f'Failed to delete {del_email}, please try again')
        else:
            st.success(f'{del_email} deleted sucessfully')
    else:
        pass

def get_bestcase():
    c.execute('SELECT * FROM projection')
    data = c.fetchall()
    return data


def create_weeklynewold_merch():
    c.execute('CREATE TABLE IF NOT EXISTS newmerch(id SERIAL PRIMARY KEY, new VARCHAR(75) UNIQUE)')
    c.execute('CREATE TABLE IF NOT EXISTS oldmerch(id SERIAL PRIMARY KEY, old VARCHAR(75) UNIQUE)')

def get_weeklynewold_merch(new_old):
    if new_old == 'new':
        c.execute('SELECT * FROM newmerch')

    elif new_old == 'old':
        c.execute('SELECT * FROM oldmerch')

    data = c.fetchall()
    return data



def update_weeklynewold_merch(new_old, merch_name2):
    if 'All' not in merch_name2:
        if new_old == 'new':
            try:
                c.execute('INSERT INTO newmerch(new) VALUES (%s)',(merch_name2[0]))
            except Exception:
                st.info('Merchant Already Exists')
        elif new_old == 'old':
            try:
                c.execute('INSERT INTO oldmerch(old) VALUES (%s)',(merch_name2[0]))
            except Exception:
                st.info('Merchant Already Exists')

    else:
        st.warning('Please input merchants one at a time, and unselect the All option')

def delete_weeklynewold_merch(new_old,del_merch_name2):
    if del_merch_name2:
        try:
            if new_old == 'new':
                for name in del_merch_name2:
                    c.execute('DELETE FROM newmerch WHERE new = %s',([name]))
            elif new_old == 'old':
                for name in del_merch_name2:
                    c.execute('DELETE FROM oldmerch WHERE old = %s',([name]))
        except Exception:
            st.warning(f'{del_merch_name2} Failed to delete Merchant, please try again')
        else:
            st.success(f'{del_merch_name2} deleted sucessfully')
    else:
        pass  




data_table()  

create_usertable()

create_targetable()

create_notes()

create_vertargetable()

create_bestcase()

create_livetargetable()

create_weeklynewold_merch()


dfmain = get_data(conn)


todaydate = datetime.date.today() - datetime.timedelta(days=2)  
today1 = st.date_input('Date', todaydate)
today = today1.strftime("%d-%b-%Y")
todaystr = today1.strftime("%a")

yesterday1, yest, yesstr = yesterday_dates(today1)

thisweek, lastweek, lastweekyear = week_dates()

lastmonth1, lastmonth, month, thismonth, lastmonth2 = month_dates(today1)

year, numofdays, lastnumofdays, daysinyr, daysleft = year_dates(today1)

condition1, condition2 = conditions(dfmain,today1,yesterday1,lastmonth1,year)

dftoday = df_today(dfmain,condition1)   

dfsum = df_sum(dftoday,todaystr,today,yesstr,yest)

mtdsumthis, mtdsumlast, runrate, runratelast, dfmtd = mtd(dfmain,today1,condition2,lastmonth,numofdays,lastnumofdays)

ytdsum, fyrunrate = ytd(dfmain,today1,daysleft)

lastmonthtarget, monthtarget, yeartarget = get_target()[0][1:4]


    

st.image(img, width=300)

st.title('Commercial Reports')

menu = ['Home','Login','SignUp']
reports = ['Daily Report', 'Weekly Report','SME Report','Barter Report','Budget Performance Report','Pipeline Performance Report','Account Management Report','User Profiles']
teams = ['Commercial','Head AM','Barter','Agency','SME & SMB','Ent & NFIs','Betting/Gaming','FMCG','IMTO','PSP','Kenya','Ghana','Uganda','Europe','SA','Zambia','Rwanda']
choice = st.sidebar.selectbox('Menu',menu)
all_team = dfmain.Vertical.value_counts().index.insert(0,'All').tolist()
if choice == 'Home':
    st.subheader('Home Page')

elif choice == 'Login':
    session_state = SessionState.get(checkboxed=False)
    st.sidebar.markdown("---")
    email = st.sidebar.text_input('Email','Enter Email Here..')
    password = st.sidebar.text_input('Password','Enter Password Here..',type='password') 
    if st.sidebar.button('Login') or session_state.checkboxed:
        result = login_user(email, password)
        if result:
            session_state.checkboxed = True
            st.sidebar.success(f'Logged In As {email.title().split("@")[0]}')
            if result[0][2] == 'Commercial' and password == result[0][3].lower():
                team_name = [result[0][2]]
                report = st.sidebar.radio('Navigation', reports)
                st.sidebar.markdown("---")
                st.subheader('Report Section')
                st.markdown('---')
                if report == 'Daily Report':
                    st.subheader(f'Daily Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')
                    st.markdown('---')
                    with st.beta_expander("Enter Targets Here"):  
                        cola, colb, colc = st.beta_columns(3)
                        lastmonthtarget1 = cola.number_input(f"What was target for last month",value=lastmonthtarget)
                        monthtarget1 = colb.number_input(f"What is target for {month}",value=monthtarget)
                        yeartarget1 = colc.number_input(f"What is {year} target",value=yeartarget)
                        update_target(lastmonthtarget1,monthtarget1,yeartarget1)

                    #card_indicators(value=10,ref=5,title='')
                    #write(f'{month} MTD:  \n ${mtdsumthis:,} - {round(mtdsumthis*100/monthtarget)}%')
                    #col1.write(f'{lastmonth} MTD:  \n ${mtdsumlast:,} - {round(mtdsumlast*100/lastmonthtarget)}%')
                    #col1.write(f'{lastmonth} Projection:  \n ${runratelast:,} - {round(runratelast*100/lastmonthtarget)}%')
                    
                    #col1.write(f'{month} YTD:  \n ${ytdsum:,} - {round(ytdsum*100/yeartarget)}%')
                    #col1.write(f'{year} FY Run Rate:  \n ${fyrunrate:,} - {round(fyrunrate*100/yeartarget)}%')
                    col1a, col3a, col2a, col4a = st.beta_columns([1.5,0.5,5.5,0.5])
                    col1a.plotly_chart(card_indicators(value=mtdsumthis,ref=monthtarget,title=f'{month} MTD',color=2))
                    col1a.plotly_chart(card_indicators(value=runrate,ref=monthtarget,title=f'{month} Run Rate',rel=True,color=1))
                    col1a.plotly_chart(card_indicators(value=ytdsum,ref=yeartarget,title=f'{month} YTD',color=2))
                    col1a.plotly_chart(card_indicators(value=fyrunrate,ref=yeartarget,title=f'{year} FY Run Rate',rel=True,color=1))
                    
                    dfmtdfig = table_fig(dfmtd,wide=1000,long=450,title='MTD Table')
                    col2a.plotly_chart(dfmtdfig)
                    col2a.markdown(get_table_download_link(dfmtd,'MTD Table'), unsafe_allow_html=True)

                    st.markdown('---')

                    dfsumfig = table_fig(dfsum,long=700,title='Product Performance Table')
                    st.plotly_chart(dfsumfig)     
                    st.markdown(get_table_download_link(dfsum,'Product Performance Table'), unsafe_allow_html=True)
                    
                    
                    st.markdown('---')
                    
                    mtdfig, ytdfig = daily_report_graphs(month,runrate,monthtarget,mtdsumthis,year,fyrunrate,yeartarget,ytdsum)
                    st.plotly_chart(mtdfig)
                    st.markdown('---')
                   
                    st.plotly_chart(ytdfig)
                    st.markdown('---')
                    st.subheader(f'Road to {month} {year} - ${monthtarget/1000000:,}m Tracker')
                    col11, col12, col13, col14 = st.beta_columns(4)
                    all_mer = dfmain.MerchName2.value_counts().index.insert(0,'All' ).tolist()
                    merch_name = col11.multiselect('Search Merchants To Add/Update',all_mer,default=['All'])
                    st.info('Please Enter Merchants and Best Case one at a time')
                    best_fig = col12.number_input('Input Best Case',value=1)
                    update_bestcase(merch_name, best_fig)
                    dfpro1 = get_bestcase()
                    dfpro = pd.DataFrame(dfpro1, columns=['sn','MerchName2','bestCase'])
                    dfpro = dfpro.iloc[:,1:]
                    del_merch_name = col14.multiselect('Search Merchants to Delete..',dfpro.MerchName2.tolist())
                    delete_bestcase(del_merch_name)
                    dfprojection = projection(dfpro,numofdays,dfmain,today1)
                    dfprojectionfig = table_fig(dfprojection)
                    st.plotly_chart(dfprojectionfig)
                    st.markdown(get_table_download_link(dfprojection,f'Road to {month} {year} - ${monthtarget/1000000:,}m Tracker'), unsafe_allow_html=True)

                    st.markdown('---')
                    st.subheader('Daily Notes')
                    
                    try:
                        st.write(f'{view_notes(today1,"DailySummary")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(today1,"DailySummary")[0][2]}')
                    except Exception:
                        st.warning(f'No notes for {today1}')

                    with st.beta_expander("Enter Daily Summary Note Here"): 
                        notedaily = st.text_area(f'Enter daily note for {today1.strftime("%d-%B-%Y")}') 
                        edit_notes(today1, notedaily,"DailySummary")

                    with st.beta_expander("Note DataFrame"):
                        product_selected = st.multiselect('Select Products..', ['Collections','Payouts','FX'])
                        no_of_merch = st.slider('Number of Merchants...',1,5)
                        metric = st.radio('Select Metrics',['Rev$','TPV$','TPC'])
                        st.table(daily_product_notes(dfmain, today1, yesterday1, yesstr, yest, todaystr, today, metric, product_selected, no_of_merch))
                        st.markdown(get_table_download_link(daily_product_notes(dfmain, today1, yesterday1, yesstr, yest, todaystr, today, metric, product_selected, no_of_merch),'Notes Table'), unsafe_allow_html=True)
                    
                    #HtmlFile = open("test.html", 'r', encoding='utf-8')
                    #source_code = HtmlFile.read() 
                    #components.html(source_code,height=600)
    

                elif report == 'Weekly Report':
                    st.subheader(f'Weekly Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')
                    st.subheader(f'Week {thisweek} Summary')
                    st.markdown('---')
                    try:
                        df1, df2, dfweek, dflastweek, dfweeksum, weekStat, dfweeklyrev, dfweeksum, weekStat2 = week_summary(dfmain,today1,year,lastweekyear,thisweek,lastweek,thismonth,lastmonth,numofdays)
                        dfweeklyrevexFX, weekStatexFX, weekStatexFX2 = week_exfx_summary(dfmain,year,dfweek,dflastweek,thisweek,thismonth,numofdays)
                        dfweekCol, weekStatCol, weekStatCol2 = week_colpay_summary('Collections',year,dfmain,dfweek,dflastweek,thisweek)
                        dfweekPay, weekStatPay, weekStatPay2 = week_colpay_summary('Payouts',year,dfmain,dfweek,dflastweek,thisweek)
                        dfB, dfweekrevBar, dfweektpvBar, weekStatBar, weekStatBar2 = week_barter_performance(dfmain,lastweekyear,year,thisweek,lastweek,dfweek,dflastweek)
                        dfagency,weekagencyStat,weekagencyStat2 = pos_agency(dfmain,lastweekyear,thisweek,lastweek,year)
                        dfcurBothF,dfrevCur = currency_performance(dfmain,lastweekyear,thisweek,lastweek,year)
                        dfcoh,cohanalStat = cohort_analysis(dfmain,year,lastweekyear,thisweek, lastweek)
                        weeklysumfig,weeklyrevfig,weeklyrevexFXfig,weeklyrevColfig,weeklyrevPayfig,weeklytpvColfig,weeklytpvPayfig,weeklytpcColfig,weeklytpcPayfig,weeklyrevBarfig,weeklytpvBarfig,agencyrevfig,agencytpvfig,weeklyrevCurfig = weekly_report_graphs(thisweek,lastweek,dfweeksum,dfweeklyrev,dfweeklyrevexFX,dfweekCol,dfweekPay,dfweekrevBar,dfweektpvBar,dfagency,dfrevCur)
                    except Exception:
                        thisweek-=1
                        lastweek-=1
                        df1, df2, dfweek, dflastweek, dfweeksum, weekStat, dfweeklyrev, dfweeksum, weekStat2 = week_summary(dfmain,today1,year,lastweekyear,thisweek,lastweek,thismonth,lastmonth,numofdays)
                        dfweeklyrevexFX, weekStatexFX, weekStatexFX2 = week_exfx_summary(dfmain,year,dfweek,dflastweek,thisweek,thismonth,numofdays)
                        dfweekCol, weekStatCol,weekStatCol2 = week_colpay_summary('Collections',year,dfmain,dfweek,dflastweek,thisweek)
                        dfweekPay, weekStatPay,weekStatPay2 = week_colpay_summary('Payouts',year,dfmain,dfweek,dflastweek,thisweek)
                        dfB, dfweekrevBar, dfweektpvBar, weekStatBar, weekStatBar2 = week_barter_performance(dfmain,lastweekyear,year,thisweek,lastweek,dfweek,dflastweek)
                        dfagency,weekagencyStat,weekagencyStat2 = pos_agency(dfmain,lastweekyear,thisweek,lastweek,year)
                        dfcurBothF,dfrevCur = currency_performance(dfmain,lastweekyear,thisweek,lastweek,year)
                        dfcoh,cohanalStat = cohort_analysis(dfmain,year,lastweekyear,thisweek, lastweek)
                        weeklysumfig,weeklyrevfig,weeklyrevexFXfig,weeklyrevColfig,weeklyrevPayfig,weeklytpvColfig,weeklytpvPayfig,weeklytpcColfig,weeklytpcPayfig,weeklyrevBarfig,weeklytpvBarfig,agencyrevfig,agencytpvfig,weeklyrevCurfig = weekly_report_graphs(thisweek,lastweek,dfweeksum,dfweeklyrev,dfweeklyrevexFX,dfweekCol,dfweekPay,dfweekrevBar,dfweektpvBar,dfagency,dfrevCur)

                    col1, col2, col3, col4, col5 = st.beta_columns(5)
                    cola, colb = st.beta_columns(2)
                    col1.plotly_chart(card_indicators2(value=weekStat[0],ref=weekStat2[0],title=f'Revenue',rel=True,color=1))
                    col2.plotly_chart(card_indicators2(value=weekStat[1],ref=weekStat2[1],title=f'TPV',rel=True,color=2))
                    col3.plotly_chart(card_indicators2(value=weekStat[2],ref=weekStat2[2],title=f'TPC',rel=True,color=1))
                    col4.plotly_chart(card_indicators(value=weekStat[3],ref=monthtarget,title=f'{month} Month Target',rel=True,color=2))
                    col5.plotly_chart(card_indicators(value=weekStat[4],ref=monthtarget,title=f'{month} Run Rate',rel=True,color=1))
                    
                    cola.plotly_chart(weeklysumfig)
                    colb.plotly_chart(weeklyrevfig)
                    st.subheader('Weekly Notes')
                    try:
                        st.write(f'{view_notes(today1,"WeeklySummary")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(today1,"WeeklySummary")[0][2]}')
                    except Exception:
                        st.warning(f'No notes for {today1}')
                    with st.beta_expander("Enter Weekly Performance Notes Here"):   
                        noteweek = st.text_area(f'Enter weekly note for {today1.strftime("%d-%B-%Y")}') 
                        edit_notes(today1, noteweek,"WeeklySummary")
                    st.markdown('---')
                    st.subheader('ex.FX Summary')
                    st.markdown('---')
                    col1a, col1b, col1c, col1d, col1e = st.beta_columns(5)
                    col1a.plotly_chart(card_indicators2(value=weekStatexFX[0],ref=weekStatexFX[0],title=f'Revenue',rel=True,color=1))
                    col1b.plotly_chart(card_indicators2(value=weekStatexFX[1],ref=weekStatexFX[1],title=f'TPV',rel=True,color=2))
                    col1c.plotly_chart(card_indicators2(value=weekStatexFX[2],ref=weekStatexFX[2],title=f'TPC',rel=True,color=1))
                    col1d.plotly_chart(card_indicators(value=weekStatexFX[3],ref=monthtarget,title=f'{month} Month Target',rel=True,color=2))
                    col1e.plotly_chart(card_indicators(value=weekStatexFX[4],ref=monthtarget,title=f'{month} Run Rate',rel=True,color=1))

                    st.plotly_chart(weeklyrevexFXfig)
                    st.markdown('---')
                    st.subheader('Collections Performance')
                    col1w, col1x, col1y = st.beta_columns(3)
                    col1w.plotly_chart(card_indicators2(value=weekStatCol[0],ref=weekStatCol2[0],title=f'Revenue',rel=True,color=1))
                    col1x.plotly_chart(card_indicators2(value=weekStatCol[1],ref=weekStatCol2[1],title=f'TPV',rel=True,color=2))
                    col1y.plotly_chart(card_indicators2(value=weekStatCol[2],ref=weekStatCol2[2],title=f'TPC',rel=True,color=1))
                    
                    st.plotly_chart(weeklyrevColfig)
                    st.plotly_chart(weeklytpvColfig)
                    st.plotly_chart(weeklytpcColfig)
                    st.markdown('---')
                    st.subheader('Payouts Performance')
                    col1w, col1x, col1y = st.beta_columns(3)
                    col1w.plotly_chart(card_indicators2(value=weekStatPay[0],ref=weekStatPay2[0],title=f'Revenue',rel=True,color=1))
                    col1x.plotly_chart(card_indicators2(value=weekStatPay[1],ref=weekStatPay2[1],title=f'TPV',rel=True,color=2))
                    col1y.plotly_chart(card_indicators2(value=weekStatPay[2],ref=weekStatPay2[2],title=f'TPC',rel=True,color=1))
                    
                
                    st.plotly_chart(weeklyrevPayfig)
                    st.plotly_chart(weeklytpvPayfig)
                    st.plotly_chart(weeklytpcPayfig)
                    st.markdown('---')
                    st.subheader('Barter Performance')
                    col1w, col1x, col1y = st.beta_columns(3)
                    col1w.plotly_chart(card_indicators2(value=weekStatBar[0],ref=weekStatBar2[0],title=f'Revenue',rel=True,color=1))
                    col1x.plotly_chart(card_indicators2(value=weekStatBar[1],ref=weekStatBar2[1],title=f'TPV',rel=True,color=2))
                    col1y.plotly_chart(card_indicators2(value=weekStatBar[2],ref=weekStatBar2[2],title=f'TPC',rel=True,color=1))
                  
                    dfBfig = table_fig(dfB,long=500)
                    st.plotly_chart(dfBfig)
                    st.plotly_chart(weeklyrevBarfig)
                    st.plotly_chart(weeklytpvBarfig)

                    st.subheader('Barter Notes')
                    try:
                        st.write(f'{view_notes(today1,"WeeklyBarter")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(today1,"WeeklyBarter")[0][2]}')
                    except Exception:
                        st.warning(f'No notes for {today1}')
                    with st.beta_expander("Enter Barter Performance Note Here"):   
                        notebar = st.text_area(f'Enter barter note for {today1.strftime("%d-%B-%Y")}') 
                        edit_notes(today1, notebar, "WeeklyBarter")
                    
                    

                    st.markdown('---')
                    st.subheader('POS – Agency Performance')
                    col1w, col1x, col1y = st.beta_columns(3)
                    col1w.plotly_chart(card_indicators2(value=weekagencyStat[0],ref=weekagencyStat2[0],title=f'Revenue',rel=True,color=1))
                    col1x.plotly_chart(card_indicators2(value=weekagencyStat[1],ref=weekagencyStat2[1],title=f'TPV',rel=True,color=2))
                    col1y.plotly_chart(card_indicators2(value=weekagencyStat[2],ref=weekagencyStat2[2],title=f'TPC',rel=True,color=1))
                    
                    st.plotly_chart(agencyrevfig)
                    st.plotly_chart(agencytpvfig)

                    st.markdown('---')
                    st.subheader('Currency Performance')
                    dfcurBothFfig = table_fig(dfcurBothF)
                    st.plotly_chart(dfcurBothFfig)
                    st.markdown(get_table_download_link(dfcurBothF,f'Currency Performance'), unsafe_allow_html=True)
                    st.plotly_chart(weeklyrevCurfig,long=150)
                    
                    try:
                        st.write(f'{view_notes(today1,"WeeklyCurrency")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(today1,"WeeklyCurrency")[0][2]}')
                    except Exception:
                        st.warning(f'No notes for {today1}')
                    with st.beta_expander("Enter Currency Performance Notes Here"): 
                        notecurr = st.text_area(f'Enter currency note for {today1.strftime("%d-%B-%Y")}') 
                        edit_notes(today1, notecurr,"WeeklyCurrency")
                        currency_selected = st.multiselect('Select Currencies',dfmain.Currency.value_counts().index.tolist())
                        dfcurnot = currency_note(dfmain,year,lastweekyear,thisweek,lastweek,currency_selected)
                        st.dataframe(dfcurnot)
                    
                    

                    st.markdown('---')
                    st.subheader('Cohort Analysis')
                    col1aa, col1bb, col1cc = st.beta_columns(3)
                    col1aa.plotly_chart(card_indicators(value=cohanalStat[0],ref=cohanalStat[3],title=f'Top 50 Merchants',rel=True,color=1))
                    col1bb.plotly_chart(card_indicators(value=cohanalStat[1],ref=cohanalStat[3],title=f'Top 20 Merchants',rel=True,color=2))
                    col1cc.plotly_chart(card_indicators(value=cohanalStat[2],ref=cohanalStat[3],title=f'Top 10 Merchants',rel=True,color=1))
                  
                    dfcohfig = table_fig(dfcoh)
                    st.plotly_chart(dfcohfig)
                    st.markdown(get_table_download_link(dfcoh,f'Cohort Analysis'), unsafe_allow_html=True)

                    st.markdown('---')
                    st.subheader('Weekly Revenue Changes – Existing Merchants')

                    merlist1 = get_weeklynewold_merch('old')
                    dfoldmer = pd.DataFrame(merlist1, columns=['sn','MerchName2'])
    
                     
                    all_mer = dfmain.MerchName2.value_counts().index.insert(0,'All' ).tolist()

                    col11, col12, col13, col14 = st.beta_columns(4)

                    st.info('Please Enter Merchants and Best Case one at a time')
                    merch_name2 = col11.multiselect('Search Exisiting Merchants To Add/Update',all_mer,default=['All'])
                    update_weeklynewold_merch('old', merch_name2)

                    del_merch_name2 = col14.multiselect('Search Exisiting Merchants to Delete..',dfoldmer.MerchName2.tolist())
                    delete_weeklynewold_merch('old',del_merch_name2)
                    
                    
                    dfoldmerch = weekly_new_old_merch(dfoldmer.MerchName2.tolist(),year,dfmain)
                    dfoldmerchfig = table_fig(dfoldmerch)

                    st.plotly_chart(dfoldmerchfig)
   


                    st.markdown('---')
                    st.subheader('Weekly Revenue Changes – New Merchants')

                    col21, col22, col23, col24 = st.beta_columns(4)

                    merlist2 = get_weeklynewold_merch('new')
                    dfnewmer = pd.DataFrame(merlist2, columns=['sn','MerchName2'])

                    st.info('Please Enter Merchants and Best Case one at a time')
                    merch_name3 = col21.multiselect('Search New Merchants To Add/Update',all_mer,default=['All'])
                    update_weeklynewold_merch('new', merch_name3)

                    del_merch_name3 = col24.multiselect('Search New Merchants to Delete..',dfnewmer.MerchName2.tolist())
                    delete_weeklynewold_merch('new',del_merch_name3)
                    
                    
                    dfnewmerch = weekly_new_old_merch(dfnewmer.MerchName2.tolist(),year,dfmain)
                    dfnewmerchfig = table_fig(dfnewmerch)


                    st.plotly_chart(dfnewmerchfig)

                elif report == 'SME Report':
                    st.subheader(f'SME Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')

                elif report == 'Barter Report':
                    st.subheader(f'Barter Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')
                
                elif report == 'Budget Performance Report':
                    
                    st.subheader(f'{result[0][2]} Budget Performance - Welcome {result[0][1].title().split("@")[0]}')
                    st.sidebar.markdown("---")
                    col1,col2,col3,col4 = st.beta_columns(4)
                    col1.plotly_chart(card_indicators(value=mtdsumthis,ref=monthtarget,title=f'{month} MTD',color=2))
                    col2.plotly_chart(card_indicators(value=runrate,ref=monthtarget,title=f'{month} Run Rate',rel=True,color=1))
                    col3.plotly_chart(card_indicators(value=ytdsum,ref=yeartarget,title=f'{month} YTD',color=2))
                    col4.plotly_chart(card_indicators(value=fyrunrate,ref=yeartarget,title=f'{year} FY Run Rate',rel=True,color=1))
                    
                    st.markdown('---')
                    col1t, col3t, col2t, col4t = st.beta_columns([1.5,0.5,5,0.5])
                    team_month = col1t.slider('Select Month..',min_value=1,max_value=12,step=1,value=(1,12))
                    col1t.markdown('---')
                    team_quar = col1t.slider('Select Quarter..',min_value=1,max_value=4,step=1,value=(1,4))
                    col1t.markdown('---')
                    team_class = col1t.multiselect('Select Classification..',['Large','Medium','Small'],default=['Large','Medium','Small'])
                    col1t.markdown('---')
                    team_cat = col1t.multiselect('Select Category..', ['Enterprise','SMB'], default=['Enterprise','SMB'])
                    col1t.markdown('---')
                    all_team = dfmain.Vertical.value_counts().index.insert(0,'All').tolist()
                    team_name = col1t.multiselect('Search Vertical..',all_team,default=['All'])
                    col1t.markdown('---')
                    if 'All' in team_name:
                        all_mer = dfmain.MerchName2.value_counts().index.insert(0,'All').tolist()
                    else:
                        all_mer = dfmain[dfmain.Vertical.isin(team_name)].MerchName2.value_counts().index.insert(0,'All').tolist()
                    team_merch = col1t.multiselect('Search Merchants..',all_mer,default=['All'])
                    
                    dfteamrev_month = team_rev(dfmain,year,team_name, team_month, team_quar, team_class, team_cat,'Month',team_merch)
                    dfteamrev_prod = team_rev(dfmain,year,team_name, team_month, team_quar, team_class, team_cat,'Product3',team_merch)
                    dfteamrev_merch = team_rev(dfmain,year,team_name, team_month, team_quar, team_class, team_cat,'MerchName2',team_merch)
                    
                    verticalprorevfig, verticalmonrevfig = vertical_budget_graphs(dfteamrev_prod,dfteamrev_month)
                    col2t.subheader('Revenue by Merchants')
                    
                    
                    dfteamrev_merchfig = table_fig(dfteamrev_merch,wide=1000)
                    col2t.plotly_chart(dfteamrev_merchfig)
                    col2t.markdown('---')
                    st.plotly_chart(verticalmonrevfig)
                    st.markdown('---')
                    st.plotly_chart(verticalprorevfig)
                
                elif report == 'Account Management Report':
                    st.subheader(f'{result[0][2]} Account Management - Welcome {result[0][1].title().split("@")[0]}')
                    st.markdown("---")
                    team_name = st.multiselect('Select Vertical',all_team,['All'])
                    dfxxgain, dfxxloss = gainers_losers(dfmain,year,thismonth,team_name)

                    st.markdown('---')
                    st.subheader('Gainers')
    
                    try:
                        st.write(f'{view_notes(today1,"AccMgtGain")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(today1,"AccMgtGain")[0][2]}')
                    except Exception:
                        st.warning(f'No notes for {today1}')

                    with st.beta_expander("Enter Gainers Summary Note Here"): 
                        gainersnote = st.text_area(f'Enter gainers note for {today1.strftime("%d-%B-%Y")}') 
                        edit_notes(today1, gainersnote,"AccMgtGain")

                    
                    gainfig = table_fig(dfxxgain)
                    st.plotly_chart(gainfig)

                    st.markdown("---")
                    st.subheader('Losers')

                    try:
                        st.write(f'{view_notes(today1,"AccMgtLoss")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(today1,"AccMgtLoss")[0][2]}')
                    except Exception:
                        st.warning(f'No notes for {today1}')

                    with st.beta_expander("Enter Losers Summary Note Here"): 
                        losersnote = st.text_area(f'Enter losers note for {today1.strftime("%d-%B-%Y")}') 
                        edit_notes(today1, losersnote,"AccMgtLoss")

                    lossfig = table_fig(dfxxloss)
                    st.plotly_chart(lossfig)
                
                elif report == 'Pipeline Performance Report':
                    dfpip = get_pipeline()
                    all_team = dfmain.Vertical.value_counts().index.insert(0,'All').tolist()
                    team_name = st.multiselect('Select Vertical',all_team,['All'])
                    st.markdown('---')
                    pipeStat, numoflive, dfpros, dfstage = process_pipeline(dfpip,team_name)
                    st.subheader(f'{result[0][2]} Pipeline Tracker - Welcome {result[0][1].title().split("@")[0]}')
                    st.markdown('---')
                    livetarget2 = st.number_input('What is the live number target',value=500)
                    edit_livetargetable(team_name, livetarget2)
                    livetarget = get_livetarget(team_name)[0][2]
                    livefig,stagefig = pipeline_tracker_graphs(numoflive,dfstage,livetarget)
                    colp1, colp3, colp2 = st.beta_columns([3,0.25,3])
                    colp1.plotly_chart(bar_indicator(value=pipeStat[1],ref=pipeStat[0],title=f'Live Expected Revenue Achieved'))
                    colp2.plotly_chart(bar_indicator(numoflive,livetarget,title='Count of Live Prospects'))
                    st.markdown('---')
                    st.subheader('Monthly Revenue by Prospects')
                    dfprosfig = table_fig(dfpros)
                    st.plotly_chart(dfprosfig)
                    st.markdown('---')
                    st.plotly_chart(stagefig)
                    
                    
                
                elif report == 'User Profiles':
                    if result[0][1].lower().split('@')[0] in ['nzubechukwu','ayo','nujie']:
                        st.subheader(f'Jude-X - User Profiles & Update Figures')
                        user_result = view_all_users()
                        user_emails = [x[1] for x in user_result]
                        del_email = st.multiselect('Select email to Delete',user_emails)
                        delete_user(del_email)
                        dfuser = pd.DataFrame(user_result,columns=['id','Email','Team','Password']) 
                        dfuserfig = table_fig(dfuser) 
                        st.markdown('---')
                        st.plotly_chart(dfuserfig)
                        st.markdown(get_table_download_link(dfuser,'User Profiles'), unsafe_allow_html=True)
                
                    else:
                        st.error('Only Admin can view this!')
                        
            elif result[0][2] == 'Head AM' and password == result[0][3].lower():
                team_name = [result[0][2]]
                report = st.sidebar.radio('Navigation', reports[5:7])

                if report == 'Account Management Report':
                    st.subheader(f'Account Management - Welcome {result[0][1].title().split("@")[0]}')
                    st.markdown("---")
                    team_name = st.multiselect('Select Vertical',all_team,['All'])
                    dfxxgain, dfxxloss = gainers_losers(dfmain,year,thismonth,team_name)
                    st.subheader('Gainers')
                    gainfig = table_fig(dfxxgain)
                    st.plotly_chart(gainfig)
                    st.markdown("---")
                    st.subheader('Losers')
                    lossfig = table_fig(dfxxloss)
                    st.plotly_chart(lossfig)
                
                elif report == 'Pipeline Performance Report':
                    dfpip = get_pipeline()
                    st.subheader(f'{result[0][2]} Pipeline Tracker - Welcome {result[0][1].title().split("@")[0]}')
                    st.markdown('---')
                    all_team = dfmain.Vertical.value_counts().index.insert(0,'All').tolist()
                    team_name = st.multiselect('Select Vertical',all_team,['All'])
                    pipeStat, numoflive, dfpros, dfstage = process_pipeline(dfpip,team_name)
                    st.markdown('---')
                    livetarget2 = st.number_input('What is the live number target',value=500)
                    edit_livetargetable(team_name, livetarget2)
                    livetarget = get_livetarget(team_name)[0][2]
                    livefig,stagefig = pipeline_tracker_graphs(numoflive,dfstage,livetarget)
                    
                    colp1, colp3, colp2 = st.beta_columns([3,0.25,3])
                    colp1.plotly_chart(bar_indicator(value=pipeStat[1],ref=pipeStat[0],title=f'Live Expected Revenue Achieved'))
                    colp2.plotly_chart(bar_indicator(numoflive,livetarget,title='Count of Live Prospects'))
                    st.markdown('---')
                    st.subheader('Monthly Revenue by Prospects')
                    prosfig = table_fig(dfpros)
                    st.plotly_chart(prosfig)
                    st.markdown('---') 
                    st.plotly_chart(stagefig)

            elif email == result[0][1].lower() and password == result[0][3].lower():
                report = st.sidebar.radio('Navigation', reports[4:6])
                team_name = result[0][2].split()

                if report == 'Budget Performance Report':
                    
                    dfmain = dfmain[dfmain.Vertical.isin(team_name)]

                    thisweek, lastweek, lastweekyear = week_dates()

                    lastmonth1, lastmonth, month, thismonth, lastmonth2 = month_dates(today1)

                    year, numofdays, lastnumofdays, daysinyr, daysleft = year_dates(today1)

                    condition1, condition2 = conditions(dfmain,today1,yesterday1,lastmonth1,year)

                    dftoday = df_today(dfmain,condition1)   

                    dfsum = df_sum(dftoday,todaystr,today,yesstr,yest)

                    mtdsumthis, mtdsumlast, runrate, runratelast, dfmtd = mtd(dfmain,today1,condition2,lastmonth,numofdays,lastnumofdays)

                    ytdsum, fyrunrate = ytd(dfmain,today1,daysleft)

                    yeartarget2= st.number_input(f"What is {year} target",value=100000)

                    edit_vertargetable(team_name, yeartarget2)

                    yeartarget = get_vertarget(team_name)[0][2]

                    st.subheader(f'{result[0][2]} Budget Performance - Welcome {result[0][1].title().split("@")[0]}')
                    st.markdown("---")
                    col1,col2,col3,col4 = st.beta_columns(4)
                    col1.plotly_chart(card_indicators(value=mtdsumthis,ref=monthtarget,title=f'{month} MTD',color=2))
                    col2.plotly_chart(card_indicators(value=runrate,ref=monthtarget,title=f'{month} Run Rate',rel=True,color=1))
                    col3.plotly_chart(card_indicators(value=ytdsum,ref=yeartarget,title=f'{month} YTD',color=2))
                    col4.plotly_chart(card_indicators(value=fyrunrate,ref=yeartarget,title=f'{year} FY Run Rate',rel=True,color=1))
                    
                    st.markdown('---')
                    col1t, col3t, col2t, col4t = st.beta_columns([1.5,0.5,5,0.5])
                    team_month = col1t.slider('Select Month..',min_value=1,max_value=12,step=1,value=(1,12))
                    col1t.markdown('---')
                    team_quar = col1t.slider('Select Quarter..',min_value=1,max_value=4,step=1,value=(1,4))
                    col1t.markdown('---')
                    team_class = col1t.multiselect('Select Classification..',['Large','Medium','Small'],default=['Large','Medium','Small'])
                    col1t.markdown('---')
                    team_cat = col1t.multiselect('Select Category..', ['Enterprise','SMB'], default=['Enterprise','SMB'])
                    col1t.markdown('---')
                    all_mer = dfmain.MerchName2.value_counts().index.insert(0,'All').tolist()
                    team_merch = col1t.multiselect('Search Merchants..',all_mer,default=['All'])
                    
                    dfteamrev_month = team_rev(dfmain,year,team_name, team_month, team_quar, team_class, team_cat,'Month',team_merch)
                    dfteamrev_prod = team_rev(dfmain,year,team_name, team_month, team_quar, team_class, team_cat,'Product3',team_merch)
                    dfteamrev_merch = team_rev(dfmain,year,team_name, team_month, team_quar, team_class, team_cat,'MerchName2',team_merch)
                    
                    verticalprorevfig, verticalmonrevfig = vertical_budget_graphs(dfteamrev_prod,dfteamrev_month)
                    col2t.subheader('Revenue by Merchants')
                    teamrev_merchfig = table_fig(dfteamrev_merch)
                    col2t.plotly_chart(teamrev_merchfig)
                    col2t.markdown('---')
                    col2t.plotly_chart(verticalmonrevfig)
                    st.markdown('---')
                    col2t.plotly_chart(verticalprorevfig)
                
                elif report == 'Pipeline Performance Report':
                    dfpip = get_pipeline()
                    pipeStat, numoflive, dfpros, dfstage = process_pipeline(dfpip,team_name)
                    st.subheader(f'{result[0][2]} Pipeline Tracker - Welcome {result[0][1].title().split("@")[0]}')
                    st.markdown('---')
                    livetarget2 = st.number_input('What is the live number target',value=500)
                    edit_livetargetable(team_name, livetarget2)
                    livetarget = get_livetarget(team_name)[0][2]
                    livefig,stagefig = pipeline_tracker_graphs(numoflive,dfstage,livetarget)
                    
                    colp1, colp3, colp2 = st.beta_columns([3,0.25,3])
                    colp1.plotly_chart(bar_indicator(value=pipeStat[1],ref=pipeStat[0],title=f'Live Expected Revenue Achieved'))
                    colp2.plotly_chart(bar_indicator(numoflive,livetarget,title='Count of Live Prospects'))
                    st.markdown('---')
                    st.subheader('Monthly Revenue by Prospects')
                    prosfig = table_fig(dfpros)
                    st.plotly_chart(prosfig)
                    st.markdown('---')
                    st.plotly_chart(stagefig)
                    

        else:
            st.warning('Please Enter valid Credentials or Sign up')
      
elif choice == 'SignUp':
    st.subheader('Create New Account')
    st.info('Please Sign Up')
    new_email = st.text_input('Email','Enter Email Address..')
    new_team = st.multiselect('Select Team...', teams,['Commercial'])
    new_password = st.text_input('Password','Enter Password Here..',type='password') 
    if st.button('SignUp'):
        if new_email.lower().split("@")[1] == 'flutterwavego.com' and new_team[0] in teams:
            add_userdata(new_email,new_team[0],new_password)
            st.success(f'Signed up as {new_email.title().split("@")[0]}')
            st.info('Go to Login Menu to login')
        else:
            st.warning(f'{new_email.title().split("@")[0]} please contact admin for authorization')  





 



