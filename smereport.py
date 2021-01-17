import streamlit as st
from utils import sme_store, sme_reclassification, update_entrpsemer, sme_country_weekrev
from graphs import clustered_graph, table_fig, card_indicators, card_indicators2
import pandas.io.sql as psql


def sme_report(conn, c, today1, thisweek, lastweek, year, lastweekyear):
    st.subheader(
        f'SME Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')

    st.markdown('---')

    ver = ['Agency', 'Betting/Gaming',
           'Ent & NFIs', 'FMCG', 'IMTO', 'None', 'PSP']

    subpro = ['Card Issuance', 'Card Maintenance',
              'Card Termination', 'Mvisa Qr Payment']

    cat = ['SME', 'SMB']

    entmer = sme_reclassification(conn, c, year, ver, cat, subpro)

    update_entrpsemer(c, entmer)

    mer = psql.read_sql('''
                        SELECT Merchants
                        FROM entrpsemertable
                        ''', conn).merchants.tolist()

    st.subheader('Store Country Analysis')

    countries = ['GH', 'KE', 'NG', 'ZA', 'ZM', 'UG']

    countriesname = ['Ghana', 'Kenya', 'Nigeria',
                     'South Africa', 'Zambia', 'Uganda']

    for i, cou in enumerate(countries):

        dfsmecou = sme_country_weekrev(conn, year, ver, mer, cat, subpro, cou)

        smecougraph = clustered_graph(dfsmecou, 'Revenue ($)', 'Transacting Merchants',
                                      grphtitle=f'Weekly Revenue Trend Vs Transacting Merchants ({countriesname[i]})', xtitle='Week', ytitle='Rev$')

        st.plotly_chart(smecougraph)

    st.markdown('---')

    st.subheader('Store Revenue Analysis')

    dfssband, dfsswk, dfnigstore, dfnonigstore, dfwksignupcou, merStat = sme_store(
        conn, thisweek, lastweek, year, lastweekyear)

    col11aa, col11bb, col11cc, col11dd, col11ee = st.beta_columns(5)
    col11aa.plotly_chart(card_indicators(
        value=merStat[0], ref=merStat[1], title='New Merchants', color=2, rel=True))
    col11bb.plotly_chart(card_indicators(
        value=merStat[1], title='Total Transacting Merchants', color=2))
    col11cc.plotly_chart(card_indicators(
        value=merStat[2], title='Merchants Revenue', rel=True, color=1))
    col11dd.plotly_chart(card_indicators(
        value=merStat[3], title='Merchants TPC', color=2))
    col11ee.plotly_chart(card_indicators(
        value=merStat[4], ref=merStat[0], title='Avg. Transaction Count Per Merchant', rel=True, color=2, percent=True))

    col1, cola, col2, colb, col3, colc = st.beta_columns(
        [3, 1, 3, 1, 3, 1])
    col1aa, col3aa, col2aa, col4aa = st.beta_columns([3.5, 0.25, 1.5, 1])
    ssbandfig = table_fig(dfssband, wide=600, long=400)
    nigstorefig = table_fig(dfnigstore, wide=500, long=400)
    nonigstorefig = table_fig(dfnonigstore, wide=500, long=400)

    col2.plotly_chart(nigstorefig)
    col3.plotly_chart(nonigstorefig)
    col2aa.plotly_chart(ssbandfig)
    ssweekfig = clustered_graph(dfsswk, 'Revenue ($)', 'Count of Store Merchants',
                                grphtitle=f'Weekly Revenue Trend Vs Count of Store Merchants', xtitle='Week', ytitle='Revenue ($)', width=900)
    col1aa.plotly_chart(ssweekfig)
