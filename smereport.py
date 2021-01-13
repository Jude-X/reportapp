import streamlit as st
from utils import sme_store, sme_reclassification, update_entrpsemer, sme_country_weekrev
from graphs import clustered_graph, table_fig
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

    col1, cola, col2, colb, col3, colc = st.beta_columns([3, 1, 3, 1, 3, 1])

    ssbandfig = table_fig(dfssband, wide=500, long=500)
    nigstorefig = table_fig(dfnigstore, wide=500, long=500)
    nonigstorefig = table_fig(dfnonigstore, wide=500, long=500)
    col1.write(merStat[0])
    col1.write(merStat[1])
    col1.write(merStat[2])
    col1.write(merStat[3])
    col2.plotly_chart(nigstorefig)
    col3.plotly_chart(nonigstorefig)
    col3.plotly_chart(ssbandfig)
