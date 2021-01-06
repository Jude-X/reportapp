import streamlit as st
from utils import team_rev
from db import edit_vertargetable, get_vertarget
from graphs import vertical_budget_graphs, table_fig, card_indicators


def budget_performance_report(conn, result, month, monthtarget, mtdsumthis, year, yeartarget, ytdsum, runrate, fyrunrate, all_mer, all_team=None):

    st.subheader(
        f'{result[0][2]} Budget Performance - Welcome {result[0][1].title().split("@")[0]}')

    if result[0][2] in ['Commercial', 'Head AM']:
        team_name = st.multiselect(
            'Select Vertical', all_team, ['All'])
    else:
        team_name = result[0][2].split()

    st.sidebar.markdown("---")
    col1, col2, col3, col4, col5 = st.beta_columns(5)

    col1.plotly_chart(card_indicators(
        value=yeartarget, ref=monthtarget, title=f'{year} Budget', color=2))
    col2.plotly_chart(card_indicators(
        value=mtdsumthis, ref=monthtarget, title=f'{month[0:3]} MTD', color=2))
    col3.plotly_chart(card_indicators(
        value=runrate, ref=monthtarget, title=f'{month[0:3]} Run Rate', rel=True, color=1))
    col4.plotly_chart(card_indicators(
        value=ytdsum, ref=yeartarget, title=f'{month[0:3]} YTD', color=2))
    col5.plotly_chart(card_indicators(
        value=fyrunrate, ref=yeartarget, title=f'{year} FY Run Rate', rel=True, color=1))

    st.markdown('---')
    col1t, col3t, col2t, col4t = st.beta_columns(
        [1, 0.5, 5.5, 0.5])
    team_month = col1t.slider(
        'Select Month..', min_value=1, max_value=12, step=1, value=(1, 12))
    col1t.markdown('---')
    team_quar = col1t.slider(
        'Select Quarter..', min_value=1, max_value=4, step=1, value=(1, 4))
    col1t.markdown('---')
    team_class = col1t.multiselect('Select Classification..', [
        'Large', 'Medium', 'Small'], default=['Large', 'Medium', 'Small'])
    col1t.markdown('---')
    team_cat = col1t.multiselect(
        'Select Category..', ['Enterprise', 'SMB'], default=['Enterprise', 'SMB'])
    col1t.markdown('---')

    team_merch = col1t.multiselect(
        'Search Merchants..', all_mer, default=['All'])

    dfteamrev_month = team_rev(
        conn, year, team_name, team_month, team_quar, team_class, team_cat, 'Month', team_merch)
    dfteamrev_prod = team_rev(
        conn, year, team_name, team_month, team_quar, team_class, team_cat, 'Product', team_merch)
    dfteamrev_merch = team_rev(
        conn, year, team_name, team_month, team_quar, team_class, team_cat, 'MerchName2', team_merch)

    verticalprorevfig, verticalmonrevfig = vertical_budget_graphs(
        dfteamrev_prod, dfteamrev_month)

    col2t.plotly_chart(verticalmonrevfig)
    col2t.markdown('---')
    col2t.plotly_chart(verticalprorevfig)

    col2t.subheader('Revenue by Merchants')

    dfteamrev_merchfig = table_fig(dfteamrev_merch, wide=700)
    col2t.plotly_chart(dfteamrev_merchfig)
