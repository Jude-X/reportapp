import pandas as pd
from utils import get_table_download_link, projection, daily_product_notes
from graphs import table_fig, card_indicators, daily_report_graphs
from db import get_bestcase, update_target, update_bestcase, delete_bestcase, edit_notes
import streamlit as st
import pandas.io.sql as psql

# @st.cache(hash_funcs={psycopg2.extensions.connection: id}, ttl=3600, show_spinner=False)


def daily_report(c, conn, result, today1, email, numofdays, yesterday1, yesstr, yest, todaystr, today, month, lastmonth1, year, lastmonthtarget, monthtarget, yeartarget, mtdsumthis, runrate, ytdsum, fyrunrate, dfmtd, dfsum, all_mer):
    st.subheader(
        f'Daily Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')
    st.markdown('---')
    if result[0][4]:
        with st.beta_expander("Enter Targets Here"):
            cola, colb, colc = st.beta_columns(3)
            lastmonthtarget1 = cola.number_input(
                f"What was target for last month", value=lastmonthtarget)
            monthtarget1 = colb.number_input(
                f"What is target for {month}", value=monthtarget)
            yeartarget1 = colc.number_input(
                f"What is {year} target", value=yeartarget)
            update_target(c, lastmonthtarget1,
                          monthtarget1, yeartarget1)

    col11aa, col11bb, col11cc, col11dd, col11ee, col11ff, col11gg, col11hh = st.beta_columns(
        8)
    col1a, col3a, col2a, col4a = st.beta_columns([2.5, 0.5, 2.5, 1])
    col11aa.plotly_chart(card_indicators(
        value=mtdsumthis, ref=monthtarget, title=f'{month[0:3]} MTD', color=2))
    col11bb.plotly_chart(card_indicators(
        value=monthtarget, ref=monthtarget, title=f'{month[0:3]} Target', color=2))
    col11cc.plotly_chart(card_indicators(
        value=runrate, ref=monthtarget, title=f'{month[0:3]} Run Rate', rel=True, color=1))
    col11dd.plotly_chart(card_indicators(
        value=round(mtdsumthis/monthtarget*100), ref=monthtarget, title=f'{month[0:3]} Target', rel=True, color=2, percent=True))

    col11ee.plotly_chart(card_indicators(
        value=ytdsum, ref=yeartarget, title=f'{year} YTD', color=2))
    col11ff.plotly_chart(card_indicators(
        value=yeartarget, ref=monthtarget, title=f'{year} Budget', color=2))
    col11gg.plotly_chart(card_indicators(
        value=fyrunrate, ref=yeartarget, title=f'{year} Run Rate', rel=True, color=1))
    col11hh.plotly_chart(card_indicators(
        value=round(ytdsum/yeartarget*100), ref=monthtarget, title=f'{year} Target', rel=True, color=1, percent=True))

    dfmtdfig = table_fig(
        dfmtd, wide=700, long=450, title='MTD Table')
    col1a.plotly_chart(dfmtdfig)
    col1a.markdown(get_table_download_link(
        dfmtd, 'MTD Table'), unsafe_allow_html=True)

    dfsumfig = table_fig(
        dfsum, long=450, wide=750, title='Product Performance Table')
    col2a.plotly_chart(dfsumfig)
    col2a.markdown(get_table_download_link(
        dfsum, 'Product Performance Table'), unsafe_allow_html=True)

    st.markdown('---')
    col1ab, col3ab, col2ab, col4ab = st.beta_columns(
        [3.5, 0.5, 3.0, 0.5])

    mtdfig, ytdfig = daily_report_graphs(
        month, runrate, monthtarget, mtdsumthis, year, fyrunrate, yeartarget, ytdsum)
    col1ab.plotly_chart(mtdfig)

    col3ab.plotly_chart(ytdfig)

    col1xx, col3yy, col2yy, col4yy = st.beta_columns(
        [3.5, 0.5, 2.5, 0.5])

    with col1xx:
        st.subheader(
            f'Road to {month} {year} - ${monthtarget/1000000:,}m Tracker')
        try:
            dfpro = get_bestcase(conn)
            dfprojection = projection(conn, dfpro, numofdays, today1)
            dfprojectionfig = table_fig(dfprojection, long=450, wide=700)
            st.plotly_chart(dfprojectionfig)
            st.markdown(get_table_download_link(
                dfprojection, f'Road to {month} {year} - ${monthtarget/1000000:,}m Tracker'), unsafe_allow_html=True)
        except:
            st.warning('No Tracker')
    with col2yy:
        try:
            st.subheader('Daily Notes')
            st.write(
                f'{view_notes(c,today1,"DailySummary")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(c,today1,"DailySummary")[0][2]}')
        except Exception:
            st.warning(f'No notes for {today1}')

    if result[0][4]:
        with st.beta_expander("Enter Best Case and Merchants"):
            col11, col12, col13, col14 = st.beta_columns(4)
            merch_name = col11.multiselect(
                'Search Merchants To Add/Update', all_mer, default=['All'])
            st.info('Please Enter Merchants and Best Case one at a time')
            best_fig = col12.number_input('Input Best Case', value=1)
            update_bestcase(c, merch_name, best_fig)
            del_merch_name = col14.multiselect(
                'Search Merchants to Delete..', dfpro.MerchName2.tolist())
            delete_bestcase(c, del_merch_name)

        with st.beta_expander("Enter Daily Summary Note Here"):
            notedaily = st.text_area(
                f'Enter daily note for {today1.strftime("%d-%B-%Y")}')
            edit_notes(c, today1, notedaily, "DailySummary")

        with st.beta_expander("Note DataFrame"):
            product_selected = st.multiselect(
                'Select Products..', ['Collections', 'Payouts', 'FX'])
            no_of_merch = st.slider('Number of Merchants...', 1, 5)
            metric = st.radio('Select Metrics', ['Rev$', 'TPV$', 'TPC'])
            st.table(daily_product_notes(conn, today1, yesterday1, yesstr,
                                         yest, todaystr, today, metric, no_of_merch, product_selected))
            st.markdown(get_table_download_link(daily_product_notes(conn, today1, yesterday1, yesstr, yest,
                                                                    todaystr, today, metric, no_of_merch, product_selected), 'Notes Table'), unsafe_allow_html=True)
