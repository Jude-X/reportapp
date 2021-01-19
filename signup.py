import streamlit as st
import bcrypt
import base64
from PIL import Image
from db import add_userdata, view_appusers, add_appuser


def sign_up(c, conn):
    st.subheader('Create New Account')

    st.markdown('---')

    with st.beta_container():

        col1, col3, col2, col4 = st.beta_columns([0.35, 0.10, 0.65, 0.10])

        with col2:
            # https://drive.google.com/file/d/1fYbdp0eQjCovvLex51_uPoUx5ZHadhux/view?usp=sharing

            st.image('https://drive.google.com/uc?id=1fYbdp0eQjCovvLex51_uPoUx5ZHadhux',
                     use_column_width=True, height=400)

        with col1:
            new_email = st.text_input('Email')
            if new_email not in (''):
                if (new_email.lower().split("@")[1] == 'flutterwavego.com'):
                    dfappusers = view_appusers(conn, new_email)
                    if dfappusers.Vertical.tolist():
                        new_team = st.multiselect(
                            'Team', dfappusers.Vertical.tolist())
                        new_password = st.text_input(
                            'Password', type='password')
                        if len(new_password) > 0 and len(new_password) < 6:
                            st.warning('Password Too Short')
                        else:
                            if st.button('SignUp'):
                                dfappusers = view_appusers(conn, new_email)
                                if (new_email.lower().split("@")[1] == 'flutterwavego.com') and (new_email in dfappusers.Email.tolist()):
                                    salt = bcrypt.gensalt()
                                    hashed = bcrypt.hashpw(
                                        new_password.encode('utf-8'), salt)
                                    try:
                                        add_userdata(
                                            c, new_email, new_team[0], hashed.decode('utf8'))
                                        st.success(
                                            f'Signed up as {new_email.title().split("@")[0]}')
                                        st.info(
                                            'Proceed to login menu to login')
                                    except:
                                        st.info(
                                            'User Already Exists Please Sign In')
                                else:
                                    st.warning(
                                        f'{new_email.title().split("@")[0]} Please Contact Admin For Authorization')
                    else:
                        st.warning(
                            'Invalid Email, Please Contact Admin For Authorization')
                else:
                    st.warning(
                        'Invalid Email, Please Contact Admin For Authorization')
