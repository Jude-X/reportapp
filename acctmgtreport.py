import streamlit as st
from utils import gainers_losers, get_table_download_link
from graphs import table_fig, acct_mgt_graphs
from db import view_notes, edit_notes
import pandas.io.sql as psql


def acct_mgt_report(c, conn, result, today1, thismonth, year, lastweekyear):

    st.subheader(
        f'Account Management - Welcome {result[0][1].title().split("@")[0]}')
    st.markdown("---")

    all_team = ['All']+psql.read_sql(
        '''SELECT DISTINCT vertical FROM datatable WHERE vertical NOT IN ('None','SME & SMB','Betting/Gaming','Agency','Barter') AND vertical IS NOT NULL ''', conn).vertical.tolist()

    col1t, col2t, col3t, col4t, col5t = st.beta_columns(5)

    team_name = col1t.multiselect(
        'Select Vertical', all_team, ['All'], key='acctmgt')

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

    all_prod = ['All', 'Collections', 'Payouts', 'Others']

    acct_prod = col2t.multiselect(
        'Search Products..', all_prod, default=['All'])

    if not acct_prod:
        acct_prod = ['All']

    acct_merch = col3t.multiselect(
        'Search Merchants..', all_mer, default=['All'])

    if not acct_merch:
        acct_merch = ['All']

    acct_curr = col4t.multiselect(
        'Search Currency..', all_curr, default=['All'])

    if not acct_curr:
        acct_curr = ['All']

    acct_metrics = col5t.radio(
        'Select Metrics', ['Rev$', 'TPV$', 'TPC'], key='vertical1')

    dfxxgain, dfxxloss = gainers_losers(
        conn, year, lastweekyear, thismonth, team_name, acct_curr, acct_prod, acct_merch, acct_metrics, all_team)

    st.markdown('---')

    try:
        st.write(
            f'{view_notes(c,today1,"AccMgtGain")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(c,today1,"AccMgtGain")[0][2]}')
    except Exception:
        st.warning(f'No notes for {today1}')

    if result[0][4]:
        with st.beta_expander("Enter Gainers Summary Note Here"):
            gainersnote = st.text_area(
                f'Enter gainers note for {today1.strftime("%d-%B-%Y")}')
            edit_notes(c, today1, gainersnote, "AccMgtGain")

    gainfig = table_fig(
        dfxxgain, title=f'Gainers By {acct_metrics}', wide=1315)
    st.plotly_chart(gainfig)
    st.markdown(get_table_download_link(
        dfxxgain, f'Gainers By {acct_metrics} Table'), unsafe_allow_html=True)

    st.markdown("---")

    try:
        st.write(
            f'{view_notes(c,today1,"AccMgtLoss")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(c,today1,"AccMgtLoss")[0][2]}')
    except Exception:
        st.warning(f'No notes for {today1}')

    if result[0][4]:
        with st.beta_expander("Enter Losers Summary Note Here"):
            losersnote = st.text_area(
                f'Enter losers note for {today1.strftime("%d-%B-%Y")}')
            edit_notes(c, today1, losersnote, "AccMgtLoss")

    lossfig = table_fig(dfxxloss, title=f'Losers By {acct_metrics}', wide=1315)
    st.plotly_chart(lossfig)
    st.markdown(get_table_download_link(
        dfxxloss, f'Losers By {acct_metrics} Table'), unsafe_allow_html=True)
