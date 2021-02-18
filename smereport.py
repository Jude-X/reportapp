import streamlit as st
from utils import sme_store, sme_reclassification, update_entrpsemer, sme_country_weekrev, get_country, sme_summary
from graphs import clustered_graph, table_fig, card_indicators, card_indicators2, vertical_bar, multiple_bar_graphs
import pandas.io.sql as psql


def sme_report(conn, c, today1, thisweek, lastweek, year, lastweekyear):
    st.warning('Report under construction')
    st.subheader(
        f'SME Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')

    st.markdown('---')

    ver = ['Agency', 'Betting/Gaming',
           'Tech & OFI', 'FMCG', 'IMTO', 'None', 'PSP']

    subpro = ['Card Issuance', 'Card Maintenance',
              'Card Termination', 'Mvisa Qr Payment']

    cat = ['SME', 'SMB']

    entmer = sme_reclassification(conn, c, year, ver, cat, subpro)

    update_entrpsemer(c, entmer)

    mer = psql.read_sql('''
                        SELECT Merchants
                        FROM entrpsemertable
                        ''', conn).merchants.tolist()

    countries = ['GH', 'KE', 'NG', 'ZA', 'ZM', 'UG']

    countriesname = ['Ghana', 'Kenya', 'Nigeria',
                     'South Africa', 'Zambia', 'Uganda']

    dfssband, dfsswk, dfnigstore, dfnonigstore, dfwksignupcou, merStat = sme_store(
        conn, thisweek, lastweek, year, lastweekyear)

    dfwksignupcou = get_country(
        conn, dfwksignupcou.loc[:, 'Country'].values.tolist(), dfwksignupcou)

    dfnigstore = get_country(
        conn, dfnigstore.loc[:, 'Country'].values.tolist(), dfnigstore, first=False)

    dfnonigstore = get_country(
        conn, dfnonigstore.loc[:, 'Country'].values.tolist(), dfnonigstore, first=False)

    dfthwksignupcou = dfwksignupcou[['Country',
                                     dfwksignupcou.columns[-1]]]

    dfthwksignupcou = dfthwksignupcou.sort_values(
        dfthwksignupcou.columns.tolist()[1], ascending=False)

    st.markdown('---')

    st.subheader('SME Summary')

    cola1, cola2, cola3, cola4 = st.beta_columns(4)

    st.markdown('---')

    colb1, colb2, colb3 = st.beta_columns(3)

    dfsmecurr, dfsmecouwks, dfsmewks, dfsmecou, dfsmepro, smeStat = sme_summary(
        conn, thisweek, lastweek, year, lastweekyear, ver, mer, cat, subpro)

    dfsmecou = get_country(
        conn, dfsmecou.loc[:, 'Country'].values.tolist(), dfsmecou, first=True)

    dfsmecou = dfsmecou.sort_values('Rev$', ascending=False)

    dfsmecoufig = vertical_bar(dfsmecou, grphtitle='Revenue Per Country',
                               xtitle='Revenue ($)', ytitle='Country', width=500)

    dfsmecurrfig = vertical_bar(dfsmecurr, grphtitle='Revenue Per Currency',
                                xtitle='Revenue ($)', ytitle='Currency', width=500)

    colb2.plotly_chart(dfsmecoufig)

    colb3.plotly_chart(dfsmecurrfig)

    st.markdown('---')

    colaa1, colbb2, colcc3 = st.beta_columns(3)

    strsgnupfig = vertical_bar(dfthwksignupcou, grphtitle='Store Merchants Signup Per Country',
                               xtitle='Count of Store Signup', ytitle='Country', width=500)

    colaa1.plotly_chart(strsgnupfig)

    st.markdown('---')

    st.subheader('SME Revenue Analysis')

    st.markdown('---')

    col11aa, col11bb, col11cc = st.beta_columns(3)
    col11aa.plotly_chart(card_indicators2(
        value=smeStat[0], ref=smeStat[1], title='Revenue($)', color=2, rel=True, nopref=True))
    col11bb.plotly_chart(card_indicators2(
        value=smeStat[2], ref=smeStat[3], title='TPV($)', color=2, rel=True, nopref=True))
    col11cc.plotly_chart(card_indicators2(
        value=smeStat[4], ref=smeStat[5], title='Merchants', color=2, rel=True, nopref=True))

    dfsmecouwks = get_country(
        conn, dfsmecouwks.loc[:, 'Country'].values.tolist(), dfsmecouwks, first=True)

    dfsmecouwksfig = multiple_bar_graphs(
        dfsmecouwks, grphtitle='Weekly Revenue Trend By Country', xtitle='Week', ytitle='Revenue ($)', width=1150)

    st.plotly_chart(dfsmecouwksfig)

    col1, col2, col3 = st.beta_columns([1, 4, 2])

    smeprofig = vertical_bar(dfsmepro, grphtitle='Revenue Per Product',
                             xtitle='Revenue', ytitle='Product', width=500)

    col1.plotly_chart(smeprofig)

    st.markdown('---')

    st.subheader('SME YTD  Trend Analysis')

    smewksfig = clustered_graph(dfsmewks, 'Revenue ($)', 'Transacting Merchants',
                                grphtitle=f'Weekly Revenue Trend Vs Transacting Merchants', xtitle='Week', ytitle='Rev$', width=1250)

    st.plotly_chart(smewksfig)

    st.markdown('---')

    st.subheader('SME Country Analysis')

    col1, col2 = st.beta_columns(2)

    for i, cou in enumerate(countries):

        dfsmecousana = sme_country_weekrev(
            conn, year, ver, mer, cat, subpro, cou)

        smecougraph = clustered_graph(dfsmecousana, 'Revenue ($)', 'Transacting Merchants',
                                      grphtitle=f'Weekly Revenue Trend Vs Transacting Merchants ({countriesname[i]})', xtitle='Week', ytitle='Rev$', width=600)

        if i % 2 == 0:
            col1.plotly_chart(smecougraph)
        else:
            col2.plotly_chart(smecougraph)

    st.markdown('---')

    st.subheader('Store Revenue Analysis')

    col11aa, col11bb, col11cc, col11dd, col11ee, col11ff, col11gg, col11hh = st.beta_columns(
        8)
    col11aa.plotly_chart(card_indicators2(
        value=merStat[0], ref=merStat[1], title='New Merchants', color=2, rel=True, nopref=True))
    col11bb.plotly_chart(card_indicators(
        value=merStat[2], title='New Transacted', color=2, nopref=True))
    col11cc.plotly_chart(card_indicators2(
        value=merStat[3], ref=merStat[4], title='Total Transacted', color=2, rel=True, nopref=True))
    col11dd.plotly_chart(card_indicators2(
        value=merStat[5], ref=merStat[6], title='Rev', color=2, rel=True))
    col11ee.plotly_chart(card_indicators2(
        value=merStat[7], ref=merStat[8], title='TPV', color=2, rel=True, nopref=True))
    col11ff.plotly_chart(card_indicators2(
        value=merStat[9], ref=merStat[10], title='TPC', color=2, rel=True, nopref=True))
    col11gg.plotly_chart(card_indicators(
        value=merStat[11], ref=merStat[0], title='Avg.TPV', rel=True, color=2))
    col11hh.plotly_chart(card_indicators(
        value=merStat[12], ref=merStat[0], title='Avg.TPC', rel=True, color=2))

    col1, colb, col2, colc = st.beta_columns(
        [7, 0.15, 3.5, 0.5])

    ssbandfig = table_fig(dfssband, wide=600, long=400)
    nigstorefig = table_fig(dfnigstore, wide=500, long=400)

    wkstresgnupfig = multiple_bar_graphs(
        dfwksignupcou, grphtitle='Weekly Signup Per Country', xtitle='Country', ytitle='Signup', width=900)
    col1.plotly_chart(wkstresgnupfig)
    col2.plotly_chart(nigstorefig)

    col1aa, colbb, col2aa, colcc = st.beta_columns(
        [7, 0.15, 3.5, 0.5])

    ssweekfig = clustered_graph(dfsswk, 'Revenue ($)', 'Count of Store Merchants',
                                grphtitle=f'Weekly Revenue Trend Vs Count of Store Merchants', xtitle='Week', ytitle='Revenue ($)', width=900)
    col1aa.plotly_chart(ssweekfig)

    nonigstorefig = table_fig(dfnonigstore, wide=500, long=400)
    col2aa.plotly_chart(nonigstorefig)

    # col2aa.plotly_chart(ssbandfig)
