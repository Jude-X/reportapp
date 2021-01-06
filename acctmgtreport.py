import streamlit as st
from utils import gainers_losers
from graphs import table_fig
from db import view_notes, edit_notes


def acct_mgt_report(c, conn, result, today1, thismonth, year, all_team):

    st.subheader(
        f'{result[0][2]} Account Management - Welcome {result[0][1].title().split("@")[0]}')
    st.markdown("---")

    team_name = st.multiselect(
        'Select Vertical', all_team, ['All'], key='acctmgt')

    dfxxgain, dfxxloss = gainers_losers(conn,
                                        year, thismonth, team_name)

    st.markdown('---')
    st.subheader('Gainers')

    try:
        st.write(
            f'{view_notes(c,today1,"AccMgtGain")[0][1].strftime("%d-%B-%Y")} Notes:  \n  \n {c,view_notes(today1,"AccMgtGain")[0][2]}')
    except Exception:
        st.warning(f'No notes for {today1}')

    if result[0][4]:
        with st.beta_expander("Enter Gainers Summary Note Here"):
            gainersnote = st.text_area(
                f'Enter gainers note for {today1.strftime("%d-%B-%Y")}')
            edit_notes(c, today1, gainersnote, "AccMgtGain")

    gainfig = table_fig(dfxxgain, wide=1350)
    st.plotly_chart(gainfig)

    st.markdown("---")
    st.subheader('Losers')

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

    lossfig = table_fig(dfxxloss, wide=1350)
    st.plotly_chart(lossfig)