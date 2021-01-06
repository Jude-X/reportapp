import streamlit as st


def barter_report(today1):
    st.subheader(
        f'Barter Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')
