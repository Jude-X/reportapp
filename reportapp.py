import pandas as pd
import psycopg2
import pandas.io.sql as psql
import SessionState
import base64
import bcrypt
import streamlit as st
import streamlit.components.v1 as components
import codecs
import datetime
import numpy as np
import calendar
from utils import team_rev, today_dates, yesterday_dates, week_dates, month_dates, year_dates, color_change, pro_color_change, df_sum, mtd, ytd, get_table_download_link, daily_product_notes, week_summary, week_exfx_summary, week_colpay_summary, week_barter_performance, pos_agency, currency_performance, currency_note, weekly_new_old_merch, cohort_analysis, get_pipeline, process_pipeline, projection, gainers_losers
from graphs import daily_report_graphs, weekly_report_graphs, vertical_budget_graphs, pipeline_tracker_graphs, card_indicators, card_indicators2, table_fig, bar_indicator
from db import data_table, create_notes, create_usertable, create_bestcase, create_storetxn, view_all_targets, login_user, create_targetable, edit_vertargetable, get_vertarget, get_target, create_vertargetable, create_livetargetable, create_weeklynewold_merch, create_appusertable, create_entrpsemertable, create_ravestore, update_target
from dailyreport import daily_report
from weeklyreport import weekly_report
from smereport import sme_report
from barterreport import barter_report
from budpermreport import budget_performance_report
from acctmgtreport import acct_mgt_report
from pipelinereport import pipeline_report
from userprofile import user_profile
from signup import sign_up
from calendar import monthrange
from PIL import Image
import plotly.graph_objects as go
from plotly import tools
import plotly.offline as py
import plotly.express as px
import os
from plotly.subplots import make_subplots
from dotenv import load_dotenv
# for python 3+ use: from urllib.parse import urlparse
from urllib.parse import urlparse

load_dotenv()
img = Image.open('flutterwavelogo2.png')

st.set_page_config(page_title='Flutterwave Report App',
                   layout='wide', initial_sidebar_state='collapsed')


# os.getenv("HEROKU_POSTGRESQL_GOLD_URL"))
result1 = urlparse(os.getenv("HEROKU_POSTGRESQL_GOLD_URL"))
# also in python 3+ use: urlparse("YourUrl") not urlparse.urlparse("YourUrl")
username = result1.username
password = result1.password
database = result1.path[1:]
hostname = result1.hostname

# Connect to the PostgreSQL database server
conn = psycopg2.connect(
    database=database,
    user=username,
    password=password,
    host=hostname
)

conn.autocommit = True
c = conn.cursor()

create_ravestore(c)

data_table(c)

create_entrpsemertable(c)

create_usertable(c)

create_targetable(c)

create_notes(c)

create_vertargetable(c)

create_bestcase(c)

create_livetargetable(c)

create_weeklynewold_merch(c)

create_appusertable(c)

create_storetxn(c)


menu = ['Home', 'Login', 'SignUp']
reports = ['Daily Report', 'Weekly Report', 'SME Report', 'Barter Report',
           'Account Management Report', 'Budget Performance Report', 'Pipeline Performance Report', 'User Profiles']

choice = st.sidebar.selectbox('Menu', menu)

teams = ['Commercial', 'Head AM'] + \
    psql.read_sql('''SELECT DISTINCT vertical FROM datatable WHERE vertical != 'None' AND vertical IS NOT NULL''',
                  conn).vertical.tolist()

# Query to get the list of merchants from the database
all_mer = ['All'] + \
    psql.read_sql('SELECT DISTINCT merchname2 FROM datatable',
                  conn).merchname2.tolist()

# Query to get the list of verticals from the database
all_team = [
    'All']+psql.read_sql('''SELECT DISTINCT vertical FROM datatable WHERE vertical != 'None' AND vertical IS NOT NULL ''', conn).vertical.tolist()

if choice == 'Home':
    st.subheader('Home Page')
    col1, col2 = st.beta_columns([1, 3])
    with col2:
        @st.cache(allow_output_mutation=True)
        def get_base64_of_bin_file(bin_file):
            with open(bin_file, 'rb') as f:
                data = f.read()
            return base64.b64encode(data).decode()

        def set_png_as_page_bg(png_file):
            bin_str = get_base64_of_bin_file(png_file)
            page_bg_img = '''
            <style>
            body {
            background-image: url("data:image/png;base64,%s");
            background-size: cover;
            }
            </style>
            ''' % bin_str

            st.markdown(page_bg_img, unsafe_allow_html=True)
            return

        set_png_as_page_bg('homepage.gif')


elif choice == 'Login':
    st.image(img, width=300)
    session_state = SessionState.get(checkboxed=False)
    st.sidebar.markdown("---")
    email = st.sidebar.text_input('Email')
    password1 = st.sidebar.text_input(
        'Password', type='password')

    signin = st.sidebar.empty()

    logged_in = bool(signin.button('Login', key='loginbutton'))

    if logged_in or session_state.checkboxed:
        result = login_user(c, email, password1)
        if result:
            session_state.checkboxed = True

            if bool(signin.button('Logout', key='logoutbutton')):
                logged_in = False
                session_state.checkboxed = False
                st.success('Logged Out Successfully')

            else:

                st.sidebar.success(
                    f'Logged In As {email.title().split("@")[0]}')

                today1, today, todaystr = today_dates(c)

                yesterday1, yest, yesstr = yesterday_dates(today1)

                thisweek, lastweek, lastweekyear = week_dates(today1)

                lastmonth1, lastmonth, month, thismonth = month_dates(
                    today1)

                year, numofdays, lastnumofdays, daysinyr, daysleft = year_dates(
                    today1)

                if result[0][2] == 'Commercial':
                    report = st.sidebar.radio('Navigation', reports)
                    st.sidebar.markdown("---")
                    dfsum = df_sum(conn, today1, yesterday1,
                                   todaystr, today, yesstr, yest)

                    mtdsumthis, mtdsumlast, runrate, runratelast, dfmtd = mtd(
                        conn, today1, lastmonth, lastmonth1, numofdays, lastnumofdays)

                    ytdsum, fyrunrate = ytd(conn, today1, daysleft)
                    try:
                        lastmonthtarget, monthtarget, yeartarget = get_target(c)[
                            0][1:4]
                    except Exception:
                        update_target(c, 8000000,
                                      8000000, 65900000)

                    if report == 'Daily Report':

                        daily_report(c, conn, result, today1, email, numofdays, yesterday1, yesstr, yest, todaystr, today, month, lastmonth1,
                                     year, lastmonthtarget, monthtarget, yeartarget, mtdsumthis, runrate, ytdsum, fyrunrate, dfmtd, dfsum, all_mer)

                    elif report == 'Weekly Report':

                        weekly_report(c, conn, result, today1, email, numofdays, yesterday1, yesstr, yest, todaystr, today, thisweek, lastweek,
                                      lastweekyear, thismonth, month, lastmonth, lastmonth1, year, lastmonthtarget, monthtarget, yeartarget, all_mer)

                    elif report == 'SME Report':

                        sme_report(conn, c, today1, thisweek,
                                   lastweek, year, lastweekyear)

                    elif report == 'Barter Report':

                        barter_report(today1)

                    elif report == 'Budget Performance Report':

                        budget_performance_report(
                            conn, result, month, monthtarget, mtdsumthis, year, yeartarget, ytdsum, runrate, fyrunrate, all_mer, all_team)

                    elif report == 'Account Management Report':

                        acct_mgt_report(c, conn, result, today1,
                                        thismonth, year, lastweekyear, all_team)

                    elif report == 'Pipeline Performance Report':

                        pipeline_report(c, result, all_team)

                    elif report == 'User Profiles':

                        user_profile(c, conn, result)

                elif result[0][2] == 'Head AM':

                    report = st.sidebar.radio('Navigation', reports[4:6])

                    if report == 'Account Management Report':

                        acct_mgt_report(c, conn, result, today1,
                                        thismonth, year, lastweekyear, all_team)

                    elif report == 'Budget Performance Report':

                        team_name = ['Ent & NFIs']

                        if result[0][4]:
                            with st.beta_expander("Enter Budget"):
                                monthtarget2 = st.number_input(
                                    f"What is {month[0:3]} target", value=0)
                                yeartarget2 = st.number_input(
                                    f"What is {year} target", value=0)
                                edit_vertargetable(
                                    c, team_name, monthtarget2, yeartarget2)

                        try:
                            monthtarget, yeartarget = get_vertarget(c, team_name)[
                                0][2:]
                        except Exception:
                            edit_vertargetable(c, team_name, 1000000, 1000000)
                            monthtarget, yeartarget = get_vertarget(c, team_name)[
                                0][2:]

                        mtdsumthis, mtdsumlast, runrate, runratelast, dfmtd = mtd(
                            conn, today1, lastmonth, lastmonth1, numofdays, lastnumofdays, team_name)

                        ytdsum, fyrunrate = ytd(
                            conn, today1, daysleft, team_name)

                        budget_performance_report(
                            conn, result, month, monthtarget, mtdsumthis, year, yeartarget, ytdsum, runrate, fyrunrate, all_mer)

                elif result[0][2] in teams:
                    email == result[0][1].lower()
                    report = st.sidebar.radio('Navigation', reports[5:7])
                    team_name = result[0][2]
                    team_name = [team_name]

                    all_mer = ['All'] + \
                        psql.read_sql('''SELECT DISTINCT merchname2 FROM datatable WHERE vertical IN %(s6)s''',
                                      conn, params={'s6': tuple(team_name)}).merchname2.tolist()

                    if report == 'Budget Performance Report':

                        if result[0][4]:
                            with st.beta_expander("Enter Budget"):
                                monthtarget2 = st.number_input(
                                    f"What is {month[0:3]} target", value=0)

                                edit_vertargetable(
                                    c, team_name, monthtarget2=monthtarget2)

                                yeartarget2 = st.number_input(
                                    f"What is {year} target", value=0)

                                edit_vertargetable(
                                    c, team_name, yeartarget2=yeartarget2)

                        try:
                            monthtarget, yeartarget = get_vertarget(c, team_name)[
                                0][2:]
                        except Exception:
                            edit_vertargetable(c, team_name, 1000000, 1000000)
                            monthtarget, yeartarget = get_vertarget(c, team_name)[
                                0][2:]

                        mtdsumthis, mtdsumlast, runrate, runratelast, dfmtd = mtd(
                            conn, today1, lastmonth, lastmonth1, numofdays, lastnumofdays, team_name)

                        ytdsum, fyrunrate = ytd(
                            conn, today1, daysleft, team_name)

                        budget_performance_report(
                            conn, result, month, monthtarget, mtdsumthis, year, yeartarget, ytdsum, runrate, fyrunrate, all_mer)

                    elif report == 'Pipeline Performance Report':

                        pipeline_report(c, result)

        else:
            if password1:
                st.warning('Please Enter valid Credentials or Sign up')


elif choice == 'SignUp':
    sign_up(c, conn, teams)
