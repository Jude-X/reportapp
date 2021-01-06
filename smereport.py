import streamlit as st


def sme_report(today1):
    st.subheader(
        f'SME Report - {today1.strftime("%A")}, {today1.strftime("%d-%B-%Y")}')
