import pandas as pd
from utils import get_table_download_link, projection
from db import view_all_users, view_all_appusers, add_appuser, delete_user, delete_appuser
from graphs import table_fig
import streamlit as st


def user_profile(c, conn, result):
    if result[0][4]:
        st.subheader(f'User Profiles')
        dfusers = view_all_users(conn)
        user_emails = dfusers.Email.tolist()
        del_email = st.multiselect(
            'Select email to Delete', user_emails)
        delete_user(c, del_email)
        dfusersfig = table_fig(
            dfusers[['Team', 'Email', 'Admin']], wide=1150)
        st.plotly_chart(dfusersfig)
        st.markdown(get_table_download_link(
            dfusers, 'Signed Up Users'), unsafe_allow_html=True)

        st.markdown('---')

        dfappusers = view_all_appusers(conn)
        appuser_emails = dfappusers.Email.tolist()
        del_appuser_email = st.multiselect(
            'Select email to Delete', appuser_emails)
        delete_appuser(c, del_appuser_email)
        dfappusersfig = table_fig(dfappusers[['Email', 'ID']], wide=550)
        st.plotly_chart(dfappusersfig)

        st.markdown(get_table_download_link(
            dfappusers, 'Approved Users'), unsafe_allow_html=True)
        appuser_email = st.text_input(
            'Email For Sign Up')
        appuser_vertical = st.text_input(
            'Vertical For Sign Up')
        add_appuser(c, appuser_email, appuser_vertical)
    else:
        st.error('Only Admin can view this!')
