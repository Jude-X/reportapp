import streamlit as st
import bcrypt
import datetime
import pandas

# \COPY datatable(TPV, TPC, Fees, Month, Date, Currency, Merchants, country, Product, Product2, Product3, TPV$, Rev$, Day, Product4, Week, Vertical, Global, Product_FX, New_Ex, MerchName2, Category, Classification, Quarter, DayOfWeek, DayName, Year) FROM 'C:\Users\Nzubechukwu Onyekaba\Desktop\project\data.csv' DELIMITER ',' CSV HEADER encoding 'UTF8';
# get data function


def data_table(c):
    c.execute(
        '''
    CREATE TABLE IF NOT EXISTS datatable(
	ID BIGSERIAL PRIMARY KEY,
	TPV DECIMAL(13,2) DEFAULT 0,
    TPC BIGINT DEFAULT 1 CHECK (TPC>=0),
    Fees DECIMAL(13,2) DEFAULT 0,
	Month SMALLINT DEFAULT NULL CHECK (Month<=12),
	Date TIMESTAMP DEFAULT NULL,
	Currency VARCHAR(4) DEFAULT NULL,
	Merchants VARCHAR(250) DEFAULT NULL,
	country VARCHAR(3) DEFAULT NULL,
	Product VARCHAR(100) DEFAULT NULL,
	Product2 VARCHAR(75) DEFAULT NULL,
	Product3 VARCHAR(25) DEFAULT NULL,
	TPV$ DECIMAL(13,2) DEFAULT 0,
	Rev$ DECIMAL(11,2) DEFAULT 0,
	Day SMALLINT DEFAULT NULL CHECK (Day<=31),
	Product4 VARCHAR(15) DEFAULT NULL,
	Week SMALLINT DEFAULT NULL CHECK (Week<=53),
	Vertical VARCHAR(25) DEFAULT NULL,
	Global VARCHAR(25) DEFAULT NULL,
	Product_FX VARCHAR(15) DEFAULT NULL,
	New_Ex VARCHAR(18) DEFAULT NULL,
	MerchName2 VARCHAR(250) DEFAULT NULL,
	Category VARCHAR(15) DEFAULT NULL,
	Classification VARCHAR(10) DEFAULT NULL,
	Quarter SMALLINT DEFAULT NULL CHECK (Quarter<=4),
	DayOfWeek SMALLINT DEFAULT NULL CHECK (DayOfWeek<=9),
	DayName VARCHAR(5) DEFAULT NULL,
	Year SMALLINT DEFAULT NULL CHECK (Year>=2016)
    )
    ''')


def create_usertable(c):
    c.execute('CREATE TABLE IF NOT EXISTS userstable(id SERIAL PRIMARY KEY, email VARCHAR(50) UNIQUE, vertical VARCHAR(25), password VARCHAR)')


def add_userdata(c, email, vertical, password):
    c.execute('INSERT INTO userstable(email,vertical,password) VALUES (%s,%s,%s)',
              (email, vertical, password))


def login_user(c, email, password):
    try:
        c.execute('SELECT * FROM userstable WHERE email = %s', ([email]))
        data = c.fetchall()
        if bcrypt.checkpw(password.encode('utf-8'), data[0][3].encode('utf-8')):
            return data
        else:
            return []
    except Exception:
        pass


def create_targetable(c):
    c.execute('CREATE TABLE IF NOT EXISTS targetable(id SERIAL PRIMARY KEY, last_month_target INTEGER, month_target INTEGER, year_target INTEGER)')


def view_all_users(c):
    c.execute('SELECT * FROM userstable')
    data = c.fetchall()
    return data


def view_all_targets(c):
    c.execute('SELECT * FROM vertargetable')
    data = c.fetchall()
    return data


def update_target(c, lastmonthtarget=0, monthtarget=0, yeartarget=0):
    if lastmonthtarget != 0:
        c.execute(
            'UPDATE targetable SET last_month_target = %s WHERE id = %s', (lastmonthtarget, 1))
    if monthtarget != 0:
        c.execute(
            'UPDATE targetable SET month_target = %s WHERE id = %s', (monthtarget, 1))
    if yeartarget != 0:
        c.execute(
            'UPDATE targetable SET year_target = %s WHERE id = %s', (yeartarget, 1))


def get_target(c):
    c.execute('SELECT * FROM targetable WHERE id = 1')
    data = c.fetchall()
    return data


def view_target(c):
    c.execute('SELECT * FROM targetable')
    data = c.fetchall()
    return data


def create_notes(c):
    c.execute('''CREATE TABLE IF NOT EXISTS dailysumnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, dailysum VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS weeklysumnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, weeklysumn VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS weeklycurrnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, weeklycurr VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS weeklybarnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, weeklybar VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS accmgtgainnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, accmgtgain VARCHAR(3000))''')
    c.execute('''CREATE TABLE IF NOT EXISTS accmgtlossnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, accmgtloss VARCHAR(3000))''')
    c.execute('''CREATE TABLE IF NOT EXISTS smesummnotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, smesumm VARCHAR(1500))''')
    c.execute('''CREATE TABLE IF NOT EXISTS pipelinenotes(id SERIAL PRIMARY KEY,date_created DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE, pipeline VARCHAR(1500))''')


def edit_notes(c, today1, note, nameofnote):
    if note:
        if datetime.datetime.now().day - today1.day >= 0:
            if nameofnote == 'DailySummary':
                try:
                    c.execute(
                        'INSERT INTO dailysumnotes(date_created,dailysum) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE dailysumnotes SET dailysum = %s WHERE date_created = %s', (note, today1))
            elif nameofnote == 'WeeklySummary':
                try:
                    c.execute(
                        'INSERT INTO weeklysumnotes(date_created,weeklysum) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE weeklysumnotes SET weeklysum = %s WHERE date_created = %s', (note, today1))
            elif nameofnote == 'WeeklyCurrency':
                try:
                    c.execute(
                        'INSERT INTO weeklycurrnotes(date_created,weeklycurr) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE weeklycurrnotes SET weeklycurr = %s WHERE date_created = %s', (note, today1))
            elif nameofnote == 'WeeklyBarter':
                try:
                    c.execute(
                        'INSERT INTO weeklybarnotes(date_created,weeklybar) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE weeklybarnotes SET weeklybar = %s WHERE date_created = %s', (note, today1))
            elif nameofnote == 'AccMgtGain':
                try:
                    c.execute(
                        'INSERT INTO accmgtgainnotes(date_created,accmgtgain) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE accmgtgainnotes SET accmgtgain = %s WHERE date_created = %s', (note, today1))
            elif nameofnote == 'AccMgtLoss':
                try:
                    c.execute(
                        'INSERT INTO accmgtlossnotes(date_created,accmgtloss) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE accmgtlossnotes SET accmgtloss = %s WHERE date_created = %s', (note, today1))
            elif nameofnote == 'SME':
                try:
                    c.execute(
                        'INSERT INTO smesummnotes(date_created,smesumm) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE smesummnotes SET smesumm = %s WHERE date_created = %s', (note, today1))
            elif nameofnote == 'Pipeline':
                try:
                    c.execute(
                        'INSERT INTO pipelinenotes(date_created,pipeline) VALUES (%s,%s)', (today1, note))
                except Exception:
                    c.execute(
                        'UPDATE pipelinenotes SET pipeline = %s WHERE date_created = %s', (note, today1))


def view_notes(c, today1, nameofnote):
    if nameofnote == 'DailySummary':
        c.execute(
            'SELECT * FROM dailysumnotes WHERE date_created = %s', ([today1]))
    elif nameofnote == 'WeeklySummary':
        c.execute(
            'SELECT * FROM weeklysumnotes WHERE date_created = %s', ([today1]))
    elif nameofnote == 'WeeklyCurrency':
        c.execute(
            'SELECT * FROM weeklycurrnotes WHERE date_created = %s', ([today1]))
    elif nameofnote == 'WeeklyBarter':
        c.execute(
            'SELECT * FROM weeklybarnotes WHERE date_created = %s', ([today1]))
    elif nameofnote == 'AccMgtGain':
        c.execute(
            'SELECT * FROM accmgtgainnotes WHERE date_created = %s', ([today1]))
    elif nameofnote == 'AccMgtLoss':
        c.execute(
            'SELECT * FROM accmgtlossnotes WHERE date_created = %s', ([today1]))
    elif nameofnote == 'SME':
        c.execute(
            'SELECT * FROM smesummnotes WHERE date_created = %s', ([today1]))
    elif nameofnote == 'Pipeline':
        c.execute(
            'SELECT * FROM pipelinenotes WHERE date_created = %s', ([today1]))
    data = c.fetchall()
    return data


def create_vertargetable(c):
    c.execute('CREATE TABLE IF NOT EXISTS vertargetable(id SERIAL PRIMARY KEY, vertical VARCHAR(55) UNIQUE, year_target INTEGER)')


def create_livetargetable(c):
    c.execute('CREATE TABLE IF NOT EXISTS livetargetable(id SERIAL PRIMARY KEY, vertical VARCHAR(55) UNIQUE, live_target INTEGER)')


def get_vertarget(c, team_name):
    c.execute('SELECT * FROM vertargetable WHERE vertical = %s', (team_name))
    data = c.fetchall()
    return data


def get_livetarget(c, team_name):
    c.execute('SELECT * FROM livetargetable WHERE vertical = %s', (team_name))
    data = c.fetchall()
    return data


def edit_vertargetable(c, team_name, yeartarget2):
    try:
        c.execute('INSERT INTO vertargetable(vertical,year_target) VALUES (%s,%s)',
                  (team_name[0], yeartarget2))
    except Exception:
        c.execute('UPDATE vertargetable SET year_target = %s WHERE vertical = %s',
                  (yeartarget2, team_name[0]))


def edit_livetargetable(c, team_name, livetarget2):
    try:
        c.execute('INSERT INTO livetargetable(vertical,live_target) VALUES (%s,%s)',
                  (team_name[0], livetarget2))
    except Exception:
        c.execute('UPDATE livetargetable SET live_target = %s WHERE vertical = %s',
                  (livetarget2, team_name[0]))


def create_bestcase(c):
    c.execute('CREATE TABLE IF NOT EXISTS projection(id SERIAL PRIMARY KEY, MerchName2 VARCHAR(75) UNIQUE, best_fig DECIMAL(9,2))')


def update_bestcase(c, merch_name, best_fig):
    if 'All' not in merch_name and best_fig != 1:
        try:
            c.execute('INSERT INTO projection(MerchName2,best_fig) VALUES (%s,%s)',
                      (merch_name[0], best_fig))
        except Exception:
            c.execute('UPDATE projection SET best_fig = %s WHERE MerchName2 = %s',
                      (best_fig, merch_name[0]))
    else:
        st.warning(
            'Please input merchants one at a time, and unselect the All option')


def delete_bestcase(c, del_merch_name):
    if del_merch_name:
        try:
            for name in del_merch_name:
                c.execute(
                    'DELETE FROM projection WHERE MerchName2 = %s', ([name]))
        except Exception:
            st.warning(
                f'{del_merch_name} Failed to delete Merchant, please try again')
        else:
            st.success(f'{del_merch_name} deleted sucessfully')
    else:
        pass


def delete_user(c, del_email):
    if del_email:
        try:
            for email in del_email:
                c.execute('DELETE FROM projection WHERE email = %s', ([email]))
        except Exception:
            st.warning(f'Failed to delete {del_email}, please try again')
        else:
            st.success(f'{del_email} deleted sucessfully')
    else:
        pass


def get_bestcase(c):
    c.execute('SELECT * FROM projection')
    data = c.fetchall()
    return data


def create_weeklynewold_merch(c):
    c.execute(
        'CREATE TABLE IF NOT EXISTS newmerch(id SERIAL PRIMARY KEY, new VARCHAR(75) UNIQUE)')
    c.execute(
        'CREATE TABLE IF NOT EXISTS oldmerch(id SERIAL PRIMARY KEY, old VARCHAR(75) UNIQUE)')


def get_weeklynewold_merch(c, new_old):
    if new_old == 'new':
        c.execute('SELECT * FROM newmerch')

    elif new_old == 'old':
        c.execute('SELECT * FROM oldmerch')

    data = c.fetchall()
    return data


def update_weeklynewold_merch(c, new_old, merch_name2):
    if 'All' not in merch_name2:
        if new_old == 'new':
            try:
                c.execute('INSERT INTO newmerch(new) VALUES (%s)',
                          (merch_name2[0]))
            except Exception:
                st.info('Merchant Already Exists')
        elif new_old == 'old':
            try:
                c.execute('INSERT INTO oldmerch(old) VALUES (%s)',
                          (merch_name2[0]))
            except Exception:
                st.info('Merchant Already Exists')

    else:
        st.warning(
            'Please input merchants one at a time, and unselect the All option')


def delete_weeklynewold_merch(c, new_old, del_merch_name2):
    if del_merch_name2:
        try:
            if new_old == 'new':
                for name in del_merch_name2:
                    c.execute('DELETE FROM newmerch WHERE new = %s', ([name]))
            elif new_old == 'old':
                for name in del_merch_name2:
                    c.execute('DELETE FROM oldmerch WHERE old = %s', ([name]))
        except Exception:
            st.warning(
                f'{del_merch_name2} Failed to delete Merchant, please try again')
        else:
            st.success(f'{del_merch_name2} deleted sucessfully')
    else:
        pass
