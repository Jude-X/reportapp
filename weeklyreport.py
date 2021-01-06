import streamlit as st
import pandas as pd
from utils import week_summary, week_exfx_summary, week_colpay_summary, week_barter_performance, pos_agency, currency_performance, cohort_analysis, get_table_download_link, weekly_new_old_merch
from graphs import weekly_report_graphs, card_indicators, card_indicators2, table_fig
from db import get_weeklynewold_merch, update_weeklynewold_merch, delete_weeklynewold_merch, edit_notes, view_notes


def weekly_report(c, conn, result, today1, email, numofdays, yesterday1, yesstr, yest, todaystr, today, thisweek, lastweek, lastweekyear, thismonth, month, lastmonth, lastmonth1, year, lastmonthtarget, monthtarget, yeartarget, all_mer):
    st.subheader(
        f'Weekly Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')
    st.markdown('---')
    st.subheader(f'Week {thisweek} Summary')
    st.markdown('---')
    try:
        df1, df2, df3, dfweek, dfweeklastyr, dflastweek, dfweeksum, weekStat, dfweeklyrev, dfweeksum, weekStat2 = week_summary(
            conn, today1, year, lastweekyear, thisweek, lastweek, thismonth, lastmonth, numofdays)
        # dfweeklyrevexFX, weekStatexFX, weekStatexFX2 = week_exfx_summary(
        # conn, today1, year, dfweek, dflastweek, thisweek, thismonth, numofdays)
        dfweekCol, weekStatCol, weekStatCol2 = week_colpay_summary(
            'Collections', conn, year, dfweek, dflastweek, thisweek)
        dfweekPay, weekStatPay, weekStatPay2 = week_colpay_summary(
            'Payouts', conn, year, dfweek, dflastweek, thisweek)
        dfB, dfweekrevBar, dfweektpvBar, weekStatBar, weekStatBar2 = week_barter_performance(
            conn, lastweekyear, year, thisweek, lastweek, dfweek, dflastweek)
        dfagency, weekagencyStat, weekagencyStat2 = pos_agency(
            conn, lastweekyear, thisweek, lastweek, year)
        dfcurBothF, dfrevCur = currency_performance(
            conn, lastweekyear, thisweek, lastweek, year)
        dfcoh, cohanalStat = cohort_analysis(
            conn, dfweek, year, lastweekyear, thisweek, lastweek)
        weeklysumfig, weeklylastyrfig, weeklyrevfig, weeklyrevColfig, weeklyrevPayfig, weeklytpvColfig, weeklytpvPayfig, weeklytpcColfig, weeklytpcPayfig, weeklyrevBarfig, weeklytpvBarfig, agencyrevfig, agencytpvfig, weeklyrevCurfig = weekly_report_graphs(
            year, thisweek, lastweek, dfweeksum, dfweeklastyr, dfweeklyrev, dfweekCol, dfweekPay, dfweekrevBar, dfweektpvBar, dfagency, dfrevCur)
    except Exception:
        thisweek -= 1
        lastweek -= 1
        df1, df2, df3, dfweek, dfweeklastyr, dflastweek, dfweeksum, weekStat, dfweeklyrev, dfweeksum, weekStat2 = week_summary(
            conn, today1, year, lastweekyear, thisweek, lastweek, thismonth, lastmonth, numofdays)
        # dfweeklyrevexFX, weekStatexFX, weekStatexFX2 = week_exfx_summary(
        # conn, today1, year, dfweek, dflastweek, thisweek, thismonth, numofdays)
        dfweekCol, weekStatCol, weekStatCol2 = week_colpay_summary(
            'Collections', conn, year, dfweek, dflastweek, thisweek)
        dfweekPay, weekStatPay, weekStatPay2 = week_colpay_summary(
            'Payouts', conn, year, dfweek, dflastweek, thisweek)
        dfB, dfweekrevBar, dfweektpvBar, weekStatBar, weekStatBar2 = week_barter_performance(
            conn, lastweekyear, year, thisweek, lastweek, dfweek, dflastweek)
        dfagency, weekagencyStat, weekagencyStat2 = pos_agency(
            conn, lastweekyear, thisweek, lastweek, year)
        dfcurBothF, dfrevCur = currency_performance(
            conn, lastweekyear, thisweek, lastweek, year)
        dfcoh, cohanalStat = cohort_analysis(
            conn, dfweek, year, lastweekyear, thisweek, lastweek)
        weeklysumfig, weeklylastyrfig, weeklyrevfig, weeklyrevColfig, weeklyrevPayfig, weeklytpvColfig, weeklytpvPayfig, weeklytpcColfig, weeklytpcPayfig, weeklyrevBarfig, weeklytpvBarfig, agencyrevfig, agencytpvfig, weeklyrevCurfig = weekly_report_graphs(
            year, thisweek, lastweek, dfweeksum, dfweeklastyr, dfweeklyrev, dfweekCol, dfweekPay, dfweekrevBar, dfweektpvBar, dfagency, dfrevCur)

    col1, col2, col3, col4, col5 = st.beta_columns(5)
    cola, cola1, colb, cola2 = st.beta_columns([3, 1, 3, 1])
    col1.plotly_chart(card_indicators2(
        value=weekStat[0], ref=weekStat2[0], title=f'Revenue', rel=True, color=1))
    col2.plotly_chart(card_indicators2(
        value=weekStat[1], ref=weekStat2[1], title=f'TPV', rel=True, color=2))
    col3.plotly_chart(card_indicators2(
        value=weekStat[2], ref=weekStat2[2], title=f'TPC', rel=True, color=1))
    col4.plotly_chart(card_indicators(
        value=round(weekStat[3]/monthtarget*100), ref=monthtarget, title=f'{month[0:3]} Target', rel=True, color=2, percent=True))
    col5.plotly_chart(card_indicators(
        value=round(weekStat[4]/monthtarget*100), ref=monthtarget, title=f'{month[0:3]} Run Rate', rel=True, color=1, percent=True))

    cola.plotly_chart(weeklysumfig)
    colb.plotly_chart(weeklylastyrfig)
    cola.plotly_chart(weeklyrevfig)
    colb.subheader('Weekly Notes')
    try:
        colb.write(
            f'{view_notes(c,today1,"WeeklySummary")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(c,today1,"WeeklySummary")[0][2]}')
    except Exception:
        colb.warning(f'No notes for {today1}')
    if result[0][4]:
        with st.beta_expander("Enter Weekly Performance Notes Here"):
            noteweek = st.text_area(
                f'Enter weekly note for {today1.strftime("%d-%B-%Y")}')
            edit_notes(c, today1, noteweek, "WeeklySummary")
    st.markdown('---')
    #st.subheader('ex.FX Summary')
    # st.markdown('---')
    #col1a, col1b, col1c, col1d, col1e = st.beta_columns(5)
    # col1a.plotly_chart(card_indicators2(
    #    value=weekStatexFX[0], ref=weekStatexFX[0], title=f'Revenue', rel=True, color=1))
    # col1b.plotly_chart(card_indicators2(
    #    value=weekStatexFX[1], ref=weekStatexFX[1], title=f'TPV', rel=True, color=2))
    # col1c.plotly_chart(card_indicators2(
    #    value=weekStatexFX[2], ref=weekStatexFX[2], title=f'TPC', rel=True, color=1))
    # col1d.plotly_chart(card_indicators(
    #    value=weekStatexFX[3], ref=monthtarget, title=f'{month} Month Target', rel=True, color=2))
    # col1e.plotly_chart(card_indicators(
    #    value=weekStatexFX[4], ref=monthtarget, title=f'{month} Run Rate', rel=True, color=1))

    # st.plotly_chart(weeklyrevexFXfig)
    # st.markdown('---')
    st.subheader('Collections Performance')
    st.markdown('---')
    col1w, col1x, col1y = st.beta_columns(3)
    col1w.plotly_chart(card_indicators2(
        value=weekStatCol[0], ref=weekStatCol2[0], title=f'Revenue', rel=True, color=1))
    col1x.plotly_chart(card_indicators2(
        value=weekStatCol[1], ref=weekStatCol2[1], title=f'TPV', rel=True, color=2))
    col1y.plotly_chart(card_indicators2(
        value=weekStatCol[2], ref=weekStatCol2[2], title=f'TPC', rel=True, color=1))

    st.plotly_chart(weeklyrevColfig)
    st.plotly_chart(weeklytpvColfig)
    st.plotly_chart(weeklytpcColfig)
    st.markdown('---')
    st.subheader('Payouts Performance')
    col1w, col1x, col1y = st.beta_columns(3)
    col1w.plotly_chart(card_indicators2(
        value=weekStatPay[0], ref=weekStatPay2[0], title=f'Revenue', rel=True, color=1))
    col1x.plotly_chart(card_indicators2(
        value=weekStatPay[1], ref=weekStatPay2[1], title=f'TPV', rel=True, color=2))
    col1y.plotly_chart(card_indicators2(
        value=weekStatPay[2], ref=weekStatPay2[2], title=f'TPC', rel=True, color=1))
    st.plotly_chart(weeklyrevPayfig)
    st.plotly_chart(weeklytpvPayfig)
    st.plotly_chart(weeklytpcPayfig)
    st.markdown('---')
    st.subheader('Barter Performance')
    col1w, col1x, col1y = st.beta_columns(3)
    col1w.plotly_chart(card_indicators2(
        value=weekStatBar[0], ref=weekStatBar2[0], title=f'Revenue', rel=True, color=1))
    col1x.plotly_chart(card_indicators2(
        value=weekStatBar[1], ref=weekStatBar2[1], title=f'TPV', rel=True, color=2))
    col1y.plotly_chart(card_indicators2(
        value=weekStatBar[2], ref=weekStatBar2[2], title=f'TPC', rel=True, color=1))

    dfBfig = table_fig(dfB, long=500)
    st.plotly_chart(dfBfig)
    st.plotly_chart(weeklyrevBarfig)
    st.plotly_chart(weeklytpvBarfig)
    st.subheader('Barter Notes')
    try:
        st.write(
            f'{view_notes(c,today1,"WeeklyBarter")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(c,today1,"WeeklyBarter")[0][2]}')
    except Exception:
        st.warning(f'No notes for {today1}')
    if result[0][4]:
        with st.beta_expander("Enter Barter Performance Note Here"):
            notebar = st.text_area(
                f'Enter barter note for {today1.strftime("%d-%B-%Y")}')
            edit_notes(c, today1, notebar, "WeeklyBarter")

    st.markdown('---')
    st.subheader('POS – Agency Performance')
    col1w, col1x, col1y = st.beta_columns(3)
    col1w.plotly_chart(card_indicators2(
        value=weekagencyStat[0], ref=weekagencyStat2[0], title=f'Revenue', rel=True, color=1))
    col1x.plotly_chart(card_indicators2(
        value=weekagencyStat[1], ref=weekagencyStat2[1], title=f'TPV', rel=True, color=2))
    col1y.plotly_chart(card_indicators2(
        value=weekagencyStat[2], ref=weekagencyStat2[2], title=f'TPC', rel=True, color=1))

    st.plotly_chart(agencyrevfig)
    st.plotly_chart(agencytpvfig)

    st.markdown('---')
    st.subheader('Currency Performance')
    dfcurBothFfig = table_fig(dfcurBothF)
    st.plotly_chart(dfcurBothFfig)
    st.markdown(get_table_download_link(
        dfcurBothF, f'Currency Performance'), unsafe_allow_html=True)
    st.plotly_chart(weeklyrevCurfig, long=150)

    try:
        st.write(
            f'{view_notes(c,today1,"WeeklyCurrency")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {view_notes(c,today1,"WeeklyCurrency")[0][2]}')
    except Exception:
        st.warning(f'No notes for {today1}')
    if result[0][4]:
        with st.beta_expander("Enter Currency Performance Notes Here"):
            notecurr = st.text_area(
                f'Enter currency note for {today1.strftime("%d-%B-%Y")}')
            edit_notes(c, today1, notecurr, "WeeklyCurrency")
            all_cur = psql.read_sql('SELECT DISTINCT currency FROM datatable',
                                    conn).currency.tolist()
            currency_selected = st.multiselect(
                'Select Currencies', all_cur)
            dfcurnot = currency_note(
                dfmain, year, lastweekyear, thisweek, lastweek, currency_selected)
            st.dataframe(dfcurnot)

    st.markdown('---')
    st.subheader('Cohort Analysis')
    col1aa, col1bb, col1cc = st.beta_columns(3)
    col1aa.plotly_chart(card_indicators(
        value=cohanalStat[0], ref=cohanalStat[3], title=f'Top 50 Merchants', rel=True, color=1))
    col1bb.plotly_chart(card_indicators(
        value=cohanalStat[1], ref=cohanalStat[3], title=f'Top 20 Merchants', rel=True, color=2))
    col1cc.plotly_chart(card_indicators(
        value=cohanalStat[2], ref=cohanalStat[3], title=f'Top 10 Merchants', rel=True, color=1))
    dfcohfig = table_fig(dfcoh)
    st.plotly_chart(dfcohfig)
    st.markdown(get_table_download_link(
        dfcoh, f'Cohort Analysis'), unsafe_allow_html=True)

    st.markdown('---')
    st.subheader('Weekly Revenue Changes – Existing Merchants')

    merlist1 = get_weeklynewold_merch(c, 'old')
    dfoldmer = pd.DataFrame(
        merlist1, columns=['sn', 'MerchName2'])

    col11, col12, col13, col14 = st.beta_columns(4)

    if result[0][4]:
        st.info('Please Enter Merchants and Best Case one at a time')
        merch_name2 = col11.multiselect(
            'Search Exisiting Merchants To Add/Update', all_mer, default=['All'])
        update_weeklynewold_merch(c, 'old', merch_name2)

        del_merch_name2 = col14.multiselect(
            'Search Exisiting Merchants to Delete..', dfoldmer.MerchName2.tolist())
        delete_weeklynewold_merch(c, 'old', del_merch_name2)

    dfoldmerch = weekly_new_old_merch(conn, dfoldmer.MerchName2.tolist(), year)
    dfoldmerchfig = table_fig(dfoldmerch, wide=1250)
    st.plotly_chart(dfoldmerchfig)

    st.markdown('---')
    st.subheader('Weekly Revenue Changes – New Merchants')

    col21, col22, col23, col24 = st.beta_columns(4)

    merlist2 = get_weeklynewold_merch(c, 'new')
    dfnewmer = pd.DataFrame(
        merlist2, columns=['sn', 'MerchName2'])
    if result[0][4]:
        st.info('Please Enter Merchants and Best Case one at a time')
        merch_name3 = col21.multiselect(
            'Search New Merchants To Add/Update', all_mer, default=['All'])
        update_weeklynewold_merch(c, 'new', merch_name3)
        del_merch_name3 = col24.multiselect(
            'Search New Merchants to Delete..', dfnewmer.MerchName2.tolist())
        delete_weeklynewold_merch(c, 'new', del_merch_name3)

    dfnewmerch = weekly_new_old_merch(conn, dfnewmer.MerchName2.tolist(), year)

    dfnewmerchfig = table_fig(dfnewmerch, wide=1250)

    st.plotly_chart(dfnewmerchfig)
