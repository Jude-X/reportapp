import streamlit as st
import pandas.io.sql as psql
import datetime
from utils import team_rev, get_table_download_link, team_daily, team_weekly, team_dailybrkdwn
from db import edit_vertargetable, get_vertarget
from graphs import vertical_budget_graphs, table_fig, card_indicators


def budget_performance_report(c, conn, result, today1, thisweek, thismonth, month, monthtarget, mtdsumthis, year, yeartarget, ytdsum, runrate, fyrunrate):

    if (result[0][2] in ['Commercial', 'Acct Mgt']):
        st.subheader(
            f'Budget Performance - Welcome {result[0][1].title().split("@")[0]}')
    else:
        st.subheader(
            f'{result[0][2]} Budget Performance - Welcome {result[0][1].title().split("@")[0]}')

    st.markdown("---")

    if (result[0][2] != 'Acct Mgt'):

        col1, col2, col3, col4, col5, col6, col7, col8 = st.beta_columns(8)

        col1.plotly_chart(card_indicators(
            value=monthtarget, ref=monthtarget, title=f'{month[0:3]} Target', color=2))
        col2.plotly_chart(card_indicators(
            value=mtdsumthis, ref=monthtarget, title=f'{month[0:3]} MTD', color=2))
        col3.plotly_chart(card_indicators(
            value=runrate, ref=monthtarget, title=f'{month[0:3]} Run Rate', rel=True, color=1))
        col4.plotly_chart(card_indicators(
            value=round(mtdsumthis/monthtarget*100), ref=monthtarget, title=f'{month[0:3]} Achieved', rel=True, color=2, percent=True))

        col5.plotly_chart(card_indicators(
            value=yeartarget, ref=monthtarget, title=f'{year} Budget', color=2))
        col6.plotly_chart(card_indicators(
            value=ytdsum, ref=yeartarget, title=f'{year} YTD', color=2))
        col7.plotly_chart(card_indicators(
            value=fyrunrate, ref=yeartarget, title=f'{year} FY Run Rate', rel=True, color=1))
        col8.plotly_chart(card_indicators(
            value=round(ytdsum/yeartarget*100), ref=monthtarget, title=f'{year} Achieved', rel=True, color=1, percent=True))

        st.markdown('---')

    else:
        col1, col2, col3, col4 = st.beta_columns(4)

    col1t, col3t, col2t, col4t = st.beta_columns(
        [1, 0.5, 5.5, 0.5])

    if result[0][2] in ['Commercial', 'Acct Mgt']:

        all_team = ['All']+psql.read_sql(
            '''SELECT DISTINCT vertical FROM datatable WHERE vertical NOT IN ('None','SME & SMB','Betting/Gaming','Agency','Barter') AND vertical IS NOT NULL ''', conn).vertical.tolist()

        team_name = col1t.multiselect(
            'Select Vertical', all_team, ['All'], key='commercial')

        if not team_name:
            team_name = ['All']

        if 'All' in team_name:
            all_mer = ['All'] + \
                psql.read_sql('''SELECT DISTINCT merchants FROM datatable WHERE vertical IN %(s1)s AND year = %(s2)s  ''',
                              conn, params={'s1': tuple(all_team), 's2': year}).merchants.tolist()
            all_curr = ['All'] + \
                psql.read_sql('SELECT DISTINCT currency FROM datatable WHERE year = %(s1)s AND vertical IN %(s2)s',
                              conn, params={'s1': year, 's2': tuple(all_team)}).currency.tolist()
        else:
            all_mer = ['All'] + \
                psql.read_sql('''SELECT DISTINCT merchants FROM datatable WHERE vertical IN %(s1)s AND year = %(s2)s  ''',
                              conn, params={'s1': tuple(team_name), 's2': year}).merchants.tolist()
            all_curr = ['All'] + \
                psql.read_sql('SELECT DISTINCT currency FROM datatable WHERE year = %(s1)s AND vertical IN %(s2)s',
                              conn, params={'s1': year, 's2': tuple(team_name)}).currency.tolist()

        col1t.markdown('---')

    else:
        team_name = [result[0][2]]

        all_mer = ['All'] + \
            psql.read_sql('''SELECT DISTINCT merchants FROM datatable WHERE vertical IN %(s1)s AND year = %(s2)s  ''',
                          conn, params={'s1': tuple(team_name), 's2': year}).merchants.tolist()

        all_curr = ['All'] + \
            psql.read_sql('SELECT DISTINCT currency FROM datatable WHERE year = %(s1)s AND vertical IN %(s2)s',
                          conn, params={'s1': year, 's2': tuple(team_name)}).currency.tolist()

    all_prod = ['Collections', 'Payouts', 'Others', 'All']

    team_month = col1t.slider(
        'Select Month..', min_value=1, max_value=12, step=1, value=(1, 12))
    col1t.markdown('---')

    team_quar = col1t.slider(
        'Select Quarter..', min_value=1, max_value=4, step=1, value=(1, 4))

    col1ta, col3ta, col2ta, col4ta, col5ta, col6ta = st.beta_columns(
        [1, 0.15, 3, 0.15, 3, 0.25])

    team_curr = col1ta.multiselect(
        'Search Currency..', all_curr, default=['All'])

    col1ta.markdown('---')

    if not team_curr:
        team_curr = ['All']

    team_metrics = col1ta.radio(
        'Select Metrics', ['Rev$', 'TPV$', 'TPC'], key='vertical1')
    col1ta.markdown('---')

    team_prod = col1ta.multiselect(
        'Search Products..', all_prod, default=['All'])

    if not team_prod:
        team_prod = ['All']

    col1ta.markdown('---')

    col1tb, col3tb, col2tb, col4tb = st.beta_columns(
        [1, 0.5, 5.5, 0.25])

    team_merch = col1tb.multiselect(
        'Search Merchants..', all_mer, default=['All'])

    if not team_merch:
        team_merch = ['All']

    col1tb.markdown('---')

    team_class = col1tb.multiselect('Select Classification..', [
        'Large', 'Medium', 'Small'], default=['Large', 'Medium', 'Small'])
    col1tb.markdown('---')

    if not team_class:
        team_class = ['Large', 'Medium', 'Small']

    if 'SME & SMB' in team_name:
        team_cat = ['SMB']
    else:
        team_cat = col1tb.multiselect(
            'Select Category..', ['Enterprise', 'SMB'], default=['Enterprise', 'SMB'])
        if not team_cat:
            team_cat = ['Enterprise', 'SMB']

    dfteamrev_month = team_rev(conn, year, team_name, team_month, team_quar,
                               team_class, team_cat, team_prod, 'Month', team_merch, team_curr, team_metrics)

    dfteamrev_prod = team_rev(conn, year, team_name, team_month, team_quar, team_class,
                              team_cat, team_prod, 'Product', team_merch, team_curr, team_metrics)

    dfteamrev_merch = team_rev(conn, year, team_name, team_month, team_quar, team_class,
                               team_cat, team_prod, 'Merchants', team_merch, team_curr, team_metrics)

    verticalprorevfig, verticalmonrevfig = vertical_budget_graphs(
        dfteamrev_prod, dfteamrev_month, team_metrics)

    col2t.plotly_chart(verticalmonrevfig)

    col2ta.plotly_chart(verticalprorevfig)

    dfteamrev_merchfig = table_fig(
        dfteamrev_merch, title=f'{team_metrics} by Merchants', wide=550, long=450)

    col5ta.plotly_chart(dfteamrev_merchfig)

    col5ta.markdown(get_table_download_link(
        dfteamrev_merch, 'Revenue by Merchants Table'), unsafe_allow_html=True)

    dfteamdaily = team_daily(conn, today1, year, team_name, team_metrics)

    dfteamdailyfig = table_fig(
        dfteamdaily, title='Revenue by Merchants DoD', wide=1250)
    st.plotly_chart(dfteamdailyfig)
    st.markdown(get_table_download_link(
        dfteamdaily, 'Daily Revenue Of Merchants Table'), unsafe_allow_html=True)

    dfteamweekly = team_weekly(
        conn, today1, thisweek, year, team_name, team_metrics)

    dfteamweeklyfig = table_fig(
        dfteamweekly, title='Revenue by Merchants WoW', wide=1000)

    col2tb.plotly_chart(dfteamweeklyfig)

    col2tb.markdown(get_table_download_link(
        dfteamweekly, 'Weekly Revenue Of Merchants Table'), unsafe_allow_html=True)

    st.markdown('---')

    col1, col2, col3, col4, col5 = st.beta_columns([1, 2, 1, 1, 1])

    c.execute('''SELECT MAX(Date) FROM datatable''')
    latestdate = c.fetchone()[0]

    vertoday1 = col1.date_input('Date', latestdate, min_value=datetime.datetime(
        datetime.datetime.now().year, 1, 1), max_value=latestdate, key='vertical')

    metrics = col5.radio('Select Metrics', ['Rev$', 'TPV$', 'TPC'])

    merch = col2.multiselect(
        'Search Merchants..', all_mer, default=['All'], key='vertical2')

    if not merch:
        merch = ['All']

    prod = col3.multiselect(
        'Search Product..', all_prod, default=['All'], key='vertical2')

    if not prod:
        prod = ['All']

    curr = col4.multiselect(
        'Search Currency..', all_curr, default=['All'], key='vertical2')

    if not curr:
        curr = ['All']

    dfverdaily = team_dailybrkdwn(
        conn, vertoday1, year, metrics, curr, merch, prod, team_name)

    dfverdailyfig = table_fig(
        dfverdaily, title='Daily Revenue by Merchants, Product and Currency', wide=1250)
    st.plotly_chart(dfverdailyfig)
    st.markdown(get_table_download_link(
        dfverdaily, 'Daily Revenue by Merchants, Product and Currency'), unsafe_allow_html=True)
