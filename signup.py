import streamlit as st
import bcrypt
import base64
from db import add_userdata, view_all_appusers, add_appuser


def sign_up(c, conn, teams):
    st.subheader('Create New Account')
    st.info('Please Sign Up')
    new_email = st.text_input('Email', 'Enter Email Address..')
    if new_email not in ('Enter Email Address..', ''):
        if (new_email.lower().split("@")[1] == 'flutterwavego.com'):
            dfappusers = view_all_appusers(conn, new_email)
            new_team = st.multiselect(
                'Select Team...', dfappusers.Vertical.tolist())
            new_password = st.text_input(
                'Password', 'Enter Password Here..', type='password')
        else:
            st.warning('Invalid Email, Please Contact Admin For Authorization')
        if len(new_password) < 6:
            st.warning('Password too short')
        else:
            if st.button('SignUp'):
                dfappusers = view_all_appusers(conn, new_email)
                if (new_email.lower().split("@")[1] == 'flutterwavego.com') and (new_email in dfappusers.Email.tolist()):
                    salt = bcrypt.gensalt()
                    hashed = bcrypt.hashpw(new_password.encode('utf-8'), salt)
                    try:
                        add_userdata(
                            c, new_email, new_team[0], hashed.decode('utf8'))
                        st.success(
                            f'Signed up as {new_email.title().split("@")[0]}')
                        st.info('Go to Login Menu to login')
                    except:
                        st.info('User Already Exists Please Sign Up')
                else:
                    st.warning(
                        f'{new_email.title().split("@")[0]} Please Contact Admin For Authorization')
