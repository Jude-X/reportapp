import streamlit as st
from db import get_livetarget, edit_livetargetable
from utils import process_pipeline, get_pipeline
from graphs import pipeline_tracker_graphs, table_fig, bar_indicator


def pipeline_report(c, result, all_team=None):
    try:
        dfpip = get_pipeline()
    except Exception:
        st.warning(
            'Oops, It appears the server at www.googleapis.com is lost, try again later')

    if result[0][2] in ['Commercial', 'Head AM']:
        st.subheader(
            f'Pipeline Tracker - Welcome {result[0][1].title().split("@")[0]}')

        if result[0][2] == 'Commercial':
            team_name = st.multiselect(
                'Select Vertical', all_team, ['All'], key='pipeline')
        else:
            team_name = ['Ent & NFIs']
    else:
        st.subheader(
            f'{result[0][2]} Pipeline Tracker - Welcome {result[0][1].title().split("@")[0]}')
        team_name = result[0][2].split()

    st.markdown('---')
    pipeStat, numoflive, dfpros, dfstage = process_pipeline(
        dfpip, team_name)

    if result[0][4]:
        with st.beta_expander("Set Live Target"):
            livetarget2 = st.number_input(
                'What is the live number target', value=500)
            edit_livetargetable(c, team_name, livetarget2)
    try:
        livetarget = get_livetarget(c, team_name)[2]
    except Exception:
        edit_livetargetable(c, team_name, 500)
        livetarget = get_livetarget(c, team_name)[2]

    livefig, stagefig = pipeline_tracker_graphs(
        numoflive, dfstage, livetarget)
    colp1, colp3, colp2, colp4 = st.beta_columns([1.5, 0.10, 3, 0.25])
    colp1.plotly_chart(bar_indicator(
        value=pipeStat[1], ref=pipeStat[0], title=f'Live Expected Revenue Achieved'))
    colp1.plotly_chart(bar_indicator(
        numoflive, livetarget, title='Count of Live Prospects'))

    colp2.subheader('Monthly Revenue by Prospects')
    dfprosfig = table_fig(dfpros, wide=900, long=700)
    colp2.plotly_chart(dfprosfig)
    st.markdown('---')
    st.plotly_chart(stagefig)
