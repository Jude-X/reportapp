from PIL import Image
import plotly.graph_objects as go
from plotly import tools
from plotly.subplots import make_subplots
import plotly.offline as py
import plotly.express as px
import pandas as pd


def daily_report_graphs(month, runrate, monthtarget, mtdsumthis, year, fyrunrate, yeartarget, ytdsum):
    '''
        This function is responsible for the mtd and ytd graphs on daily report page
    '''
    mtdfig = go.Figure(data=[
        go.Bar(name=f"{month} Projection", x=[
               runrate], base=0, marker_color='#3e3c3b', width=5, orientation="h"),
        go.Bar(name=f"{month} Target", x=[
               monthtarget], base=0, marker_color='#0099ff', width=4, orientation="h"),
        go.Bar(name=f"{month} MTD", x=[mtdsumthis], base=0, marker_color='#ff9933', width=3, orientation="h",
               text=f'{mtdsumthis:,} - {round(mtdsumthis*100/monthtarget)}%', textfont_color="white", textposition='inside')
    ])

    mtdfig.update_layout({'title': {'text': f'{month} MTD Graph', 'x': 0.5, 'xanchor': 'center'}, "xaxis": {"title": "Revenue($)", 'zeroline': False},
                          "yaxis": {"title": "Metrics", 'zeroline': False, 'showline': False, 'visible': False},
                          "autosize": True, "width": 650, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)',
                          "barmode": "relative", "legend": {"orientation": "h", "y": 1.15}})

    ytdfig = go.Figure(data=[
        go.Bar(name=f"{year} Projection @FY",
               x=[fyrunrate], base=0, marker_color='#3e3c3b', width=5, orientation="h"),
        go.Bar(name=f"{year} FY Target", x=[
               yeartarget], base=0, marker_color='#0099ff', width=4, orientation="h"),
        go.Bar(name=f"{year} FY Target", x=[ytdsum], base=0, marker_color='#ff9933', width=3, orientation="h",
               text=f'{ytdsum:,} - {round(ytdsum*100/yeartarget)}%', textfont_color="white", textposition='inside')
    ])

    ytdfig.update_layout({'title': {'text': f'{year} FY Projection Graph', 'x': 0.5, 'xanchor': 'center'}, "xaxis": {"title": "Revenue($)", 'zeroline': False},
                          "yaxis": {"title": "Metrics", 'zeroline': False, 'showline': False, 'visible': False},
                          "autosize": True, "width": 650, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)',
                          "barmode": "relative", "legend": {"orientation": "h", "y": 1.15}})

    return mtdfig, ytdfig


# Weekly Report Graphs
def weekly_report_graphs(year, thisweek, lastweek, dfweeksum, dfweeklastyr, dfweeklyrev, dfweekCol, dfweekPay, dfweekrevBar, dfweektpvBar, dfagency, dfrevCur):
    # weekly graphs
    weeklysumfig = go.Figure(data=[
        go.Bar(name=f'Week {lastweek} Revenue', x=dfweeksum.iloc[:, 0].values.tolist(), y=dfweeksum.iloc[:, 2].values.tolist(
        ), marker_color='#0099ff', text=dfweeksum.iloc[:, 2].values.tolist(), textfont_color='#0099ff', texttemplate='%{text:.2s}', textposition='outside'),
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweeksum.iloc[:, 0].values.tolist(), y=dfweeksum.iloc[:, 1].values.tolist(
        ), marker_color='#ff9933', text=dfweeksum.iloc[:, 1].values.tolist(), textfont_color='#ff9933', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklysumfig.update_layout({'barmode': 'group', 'title': 'W-O-W Revenue Change Per Channel', 'legend': {"orientation": "h", "y": 1.15},
                                "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklylastyrfig = go.Figure(data=[
        go.Bar(name=f'{year-1} Week {thisweek} Revenue', x=dfweeklastyr.iloc[:, 0].values.tolist(), y=dfweeklastyr.iloc[:, 1].values.tolist(
        ), marker_color='#0099ff', text=dfweeklastyr.iloc[:, 1].values.tolist(), textfont_color='#0099ff', texttemplate='%{text:.2s}', textposition='outside'),
        go.Bar(name=f'{year} Week {thisweek} Revenue', x=dfweeksum.iloc[:, 0].values.tolist(), y=dfweeksum.iloc[:, 1].values.tolist(
        ), marker_color='#ff9933', text=dfweeksum.iloc[:, 1].values.tolist(), textfont_color='#ff9933', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklylastyrfig.update_layout({'barmode': 'group', 'title': 'W-O-W Revenue Change Per Channel', 'legend': {"orientation": "h", "y": 1.15},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklyrevfig = go.Figure(data=[
        go.Scatter(x=dfweeklyrev.iloc[:, 0].values.tolist(), y=dfweeklyrev.iloc[:, 1].values.tolist(
        ), marker_color='#00008B', text=dfweeklyrev.iloc[:, 1].values.tolist())
    ])

    weeklyrevfig.update_layout({'title': 'Weekly Revenue Trend', "autosize": True, "xaxis": {
                               "title": "Week", "tickmode": 'linear', "dtick": 1}, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    # weeklyrevexFXfig = go.Figure(data=[
    # go.Scatter(x=dfweeklyrevexFX.iloc[:, 0].values.tolist(), y=dfweeklyrevexFX.iloc[:, 1].values.tolist(
    # ), marker_color='#00008B', text=dfweeklyrevexFX.iloc[:, 1].values.tolist())
    # ])

    # weeklyrevexFXfig.update_layout({'title': 'exFX Weekly Revenue Trend', "autosize": True, "xaxis": {
    # "title": "Week", "tickmode": 'linear', "dtick": 1}, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklyrevColfig = go.Figure(data=[
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweekCol.iloc[:, 0].values.tolist(), y=dfweekCol.iloc[:, 1].values.tolist(
        ), marker_color='#0099ff', text=dfweekCol.iloc[:, 1].values.tolist(), textfont_color='#0099ff', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklyrevColfig.update_layout({'barmode': 'group', 'title': 'Revenue Trend', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklytpvColfig = go.Figure(data=[
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweekCol.iloc[:, 0].values.tolist(), y=dfweekCol.iloc[:, 2].values.tolist(
        ), marker_color='#ff9933', text=dfweekCol.iloc[:, 2].values.tolist(), textfont_color='#ff9933', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklytpvColfig.update_layout({'barmode': 'group', 'title': 'TPV Trend', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklytpcColfig = go.Figure(data=[
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweekCol.iloc[:, 0].values.tolist(), y=dfweekCol.iloc[:, 3].values.tolist(
        ), marker_color='#00FF00', text=dfweekCol.iloc[:, 3].values.tolist(), textfont_color='#00FF00', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklytpcColfig.update_layout({'barmode': 'group', 'title': 'TPC Trend', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklyrevPayfig = go.Figure(data=[
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweekPay.iloc[:, 0].values.tolist(), y=dfweekPay.iloc[:, 1].values.tolist(
        ), marker_color='#0099ff', text=dfweekPay.iloc[:, 1].values.tolist(), textfont_color='#0099ff', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklyrevPayfig.update_layout({'barmode': 'group', 'title': 'Revenue Trend', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklytpvPayfig = go.Figure(data=[
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweekPay.iloc[:, 0].values.tolist(), y=dfweekPay.iloc[:, 2].values.tolist(
        ), marker_color='#ff9933', text=dfweekPay.iloc[:, 2].values.tolist(), textfont_color='#ff9933', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklytpvPayfig.update_layout({'barmode': 'group', 'title': 'TPV Trend', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklytpcPayfig = go.Figure(data=[
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweekPay.iloc[:, 0].values.tolist(), y=dfweekPay.iloc[:, 3].values.tolist(
        ), marker_color='#00FF00', text=dfweekPay.iloc[:, 3].values.tolist(), textfont_color='#00FF00', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklytpcPayfig.update_layout({'barmode': 'group', 'title': 'TPC Trend', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklyrevBarfig = go.Figure(data=[
        go.Bar(name=f'Week {thisweek} Revenue', x=dfweekrevBar.iloc[:, 0].values.tolist(), y=dfweekrevBar.iloc[:, 1].values.tolist(
        ), marker_color='#0099ff', text=dfweekrevBar.iloc[:, 1].values.tolist(), textfont_color='#0099ff', texttemplate='%{text:.2s}', textposition='outside')
    ])

    weeklyrevBarfig.update_layout({'barmode': 'group', 'title': 'Revenue Trend', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                   "yaxis": {"gridcolor": '#C0C0C0'}, "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    weeklytpvBarfig = px.line(dfweektpvBar, x='Week',
                              y='TPV$', color='Product', log_y=True)

    weeklytpvBarfig.update_layout({'title': 'TPV Trend', "xaxis": {"title": "Week", "dtick": 1, 'zeroline': False},
                                   "yaxis": {'zeroline': False, 'showline': False, 'visible': False},
                                   "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'
                                   })

    # Create figure with secondary y-axis
    agencyrevfig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    agencyrevfig.add_trace(
        go.Bar(name=f'Revenue - (NGN)', x=dfagency.iloc[:, 0].values.tolist(), y=dfagency.iloc[:, 1].values.tolist(), marker_color='#0099ff'), secondary_y=False)

    agencyrevfig.add_trace(
        go.Scatter(name=f'Revenue - (USD)', x=dfagency.iloc[:, 0].values.tolist(), y=dfagency.iloc[:, 2].values.tolist(), marker_color='#ff9933'), secondary_y=True)

    # Add figure title
    agencyrevfig.update_layout({'title': 'Revenue Trend in NGN & USD', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    # Set y-axes titles
    agencyrevfig.update_yaxes(
        title_text="Revenue - (NGN)", secondary_y=False, gridcolor='#C0C0C0')
    agencyrevfig.update_yaxes(title_text="Revenue - (USD)", secondary_y=True)

    # Create figure with secondary y-axis
    agencytpvfig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    agencytpvfig.add_trace(
        go.Bar(name=f'TPV - (NGN)', x=dfagency.iloc[:, 0].values.tolist(), y=dfagency.iloc[:, 4].values.tolist(), marker_color='#0099ff'), secondary_y=False)

    agencytpvfig.add_trace(
        go.Scatter(name=f'TPV - (USD)', x=dfagency.iloc[:, 0].values.tolist(), y=dfagency.iloc[:, 3].values.tolist(), marker_color='#ff9933'), secondary_y=True)

    # Add figure title
    agencytpvfig.update_layout({'title': 'TPV Trend in NGN & USD', 'legend': {"orientation": "h", "y": 1.15}, "xaxis": {"title": "Week", "tickmode": 'linear', "dtick": 1},
                                "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'})

    # Set y-axes titles
    agencytpvfig.update_yaxes(title_text="TPV - (NGN)",
                              secondary_y=False, gridcolor='#C0C0C0')
    agencytpvfig.update_yaxes(title_text="TPV - (USD)", secondary_y=True)

    weeklyrevCurfig = px.line(
        dfrevCur, x='Week', y='Rev$', color='Currency', log_y=True)

    weeklyrevCurfig.update_layout({'title': 'Revenue Trend', "xaxis": {"title": "Week", "dtick": 1, 'zeroline': False},
                                   "yaxis": {'zeroline': False, 'showline': False, 'visible': False},
                                   "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)'
                                   })

    return weeklysumfig, weeklylastyrfig, weeklyrevfig, weeklyrevColfig, weeklyrevPayfig, weeklytpvColfig, weeklytpvPayfig, weeklytpcColfig, weeklytpcPayfig, weeklyrevBarfig, weeklytpvBarfig, agencyrevfig, agencytpvfig, weeklyrevCurfig


# Vertical Budget Performance

def vertical_budget_graphs(dfteamrev_prod, dfteamrev_month):
    verticalprorevfig = go.Figure(data=[
        go.Bar(x=dfteamrev_prod.iloc[:, 1].values.tolist(), y=dfteamrev_prod.iloc[:, 0].values.tolist(), marker_color='#4169e1', orientation='h',
               text=dfteamrev_prod.iloc[:, 1].values.tolist(), textfont_color="white", texttemplate='%{text:.2s}', textposition='inside')
    ])
    verticalprorevfig.update_layout(title='Revenue By Product', xaxis=dict(title='Revenue ($)'),
                                    autosize=True, width=600, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    verticalprorevfig.update_yaxes(type='category')
    verticalprorevfig.update_xaxes(gridcolor='#C0C0C0')

    verticalmonrevfig = go.Figure(data=[
        go.Bar(x=dfteamrev_month.iloc[:, 0].values.tolist(), y=dfteamrev_month.iloc[:, 1].values.tolist(), marker_color='#4169e1',
               text=dfteamrev_month.iloc[:, 1].values.tolist(), textfont_color="white", texttemplate='%{text:.2s}', textposition='inside')
    ])

    verticalmonrevfig.update_layout(title='Revenue By Month', xaxis=dict(tickmode='linear', dtick=1),
                                    yaxis=dict(gridcolor='#C0C0C0'), autosize=True, width=1150, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    return verticalprorevfig, verticalmonrevfig


def acct_mgt_graphs(dfaccmer):
    accmonrevfig = go.Figure(data=[
        go.Bar(x=dfaccmer.iloc[:, 0].values.tolist(), y=dfaccmer.iloc[:, 1].values.tolist(), marker_color='#4169e1',
               text=dfaccmer.iloc[:, 1].values.tolist(), textfont_color="white", texttemplate='%{text:.2s}', textposition='inside')
    ])

    accmonrevfig.update_layout(title='Revenue By Month', xaxis=dict(tickmode='linear', dtick=1),
                               yaxis=dict(gridcolor='#C0C0C0'), autosize=True, width=1150, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    return accmonrevfig


def pipeline_tracker_graphs(numoflive, dfstage, livetarget=250):
    livefig = go.Figure(data=[
        go.Bar(name="Target", x=[livetarget], base=0,
               marker_color='#3e3c3b', width=5, orientation="h"),
        go.Bar(name="Live Prospects", x=[numoflive], base=0, marker_color='#0099ff', width=4, orientation="h",
               text=f'{numoflive} - {round(numoflive*100/livetarget)}%', textfont_color="white", textposition='inside')
    ])

    livefig.update_layout({"xaxis": {"title": "Count of Live Prospects", 'zeroline': False},
                           "yaxis": {'zeroline': False, 'showline': False, 'visible': False},
                           "autosize": True, "width": 1150, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)',
                           "barmode": "relative", "legend": {"orientation": "h", "y": 1.15}})

    data1 = []
    for i in dfstage.to_records(index=False):
        data1.append(go.Bar(name=f"{i[0]}", x=[i[2]], width=5, orientation="h",
                            text=f'{i[2]}%', textfont_color="white", textposition='inside'))

    stagefig = go.Figure(data=data1)

    stagefig.update_layout({'title': 'Prospective Merchants by Stage', "xaxis": {"title": "Stages", 'zeroline': False},
                            "yaxis": {'zeroline': False, 'showline': False, 'visible': False},
                            "autosize": True, "height": 400, "width": 1350, "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)',
                            "barmode": "stack", "legend": {"orientation": "h", "y": -0.18, 'traceorder': 'normal'}})

    return livefig, stagefig


def table_fig(df, wide=1000, long=500, title=''):

    for col in df.columns:
        try:
            df[col] = df[col].map("{:,.2f}".format)
            if col == 'MerchName2':
                df.rename(columns={col: 'Merchants'}, inplace=True)
        except:
            pass
    if df.shape[1]-1 == 0:
        noofcols = wide
    else:
        noofcols = int(df.shape[1]-1)
    if len(df.columns) < 3:
        width = wide//noofcols
        cw = [2*width/3, 1*width/3]
    else:
        width = wide//(noofcols-1)
        cw = [2*width, width, width, width]
    colstofor = df.columns.tolist()[1:]
    if 'Variance' in df.columns.tolist():
        df['Variance'] = pd.to_numeric(
            df['Variance'], errors='coerce').fillna(0).map("{:.0%}".format)
        #['black']*2+[['red' if  boolv else 'black' for boolv in df['col3'].str.contains('-')]]
        font_color = [
            'black']*(noofcols)+[['red' if boolv else 'green' for boolv in df['Variance'].str.contains('-')]]
    elif 'Rev$ Variance' in df.columns.tolist():
        #df['Rev$ Variance %'] = pd.to_numeric(df['Rev$ Variance %'], errors='coerce').fillna(0).map("{:.0%}".format)
        font_color = ['black']*(noofcols)+[
            ['rgb(255,0,0)' if '-' in v else 'rgb(0,128,0)' for v in df['Rev$ Variance']]]
    elif '% Achieved' in df.columns.tolist():
        df['% Achieved'] = pd.to_numeric(
            df['% Achieved'], errors='coerce').fillna(0).map("{:.0%}".format)
        font_color = ['black']*(noofcols)+[['rgb(255,0,0)' if float(v[0:-1])
                                            < 100 else 'rgb(0,128,0)' for v in df['% Achieved']]]
    else:
        font_color = ['black']*1+[['red' if boolv else 'black' for boolv in df[colstofor[i]
                                                                               ].str.contains('-')] for i in range(len(colstofor))]
    fig = go.Figure(data=[go.Table(
        columnwidth=cw,
        header=dict(values=list(df.columns),
                    fill_color='#ADADAD',
                    line_color='#7E685A',
                    font_color='white',
                    align='center'),
        cells=dict(values=[df[i] for i in df.columns],
                   fill_color=['#7395AE', ['white', 'lightgrey']
                               * int(df.shape[0]+1//2)],
                   line_color='#7E685A',
                   font=dict(color=font_color),
                   align=['left', 'right'],
                   height=30))
    ])
    fig.update_layout({'title': {'text': title, 'x': 0.5, 'xanchor': 'center'},
                       'autosize': True, 'height': long, 'width': wide})
    return fig


def card_indicators(value=10, ref=5, title='', rel=False, color=1, percent=False):
    if color == 1:
        col = 'white'
    else:
        col = 'white'
    if percent:
        fig = go.Figure(go.Indicator(
            mode="number",
            value=value,
            title={"text": title, 'font': {'size': 20,
                                           'color': 'darkgrey', 'family': 'Droid Serif'}},
            number={'suffix': "%", 'font': {'size': 30,
                                            'color': 'black', 'family': 'Droid Serif'}},
            domain={'x': [0, 1], 'y': [0, 1]}))
    else:
        fig = go.Figure(go.Indicator(
            mode="number",
            value=value,
            title={"text": title, 'font': {'size': 20,
                                           'color': 'darkgrey', 'family': 'Droid Serif'}},
            number={'prefix': "$", 'font': {'size': 30,
                                            'color': 'black', 'family': 'Droid Serif'}},
            domain={'x': [0, 1], 'y': [0, 1]}))

    fig.update_layout({'autosize': True, 'height': 90,
                       'width': 150, 'paper_bgcolor': col})

    return fig


def card_indicators2(value=10, ref=5, title='', rel=False, color=1):
    if color == 1:
        col = 'white'
    else:
        col = 'white'
    fig = go.Figure(go.Indicator(
        mode="number+delta",
        value=value,
        delta={'reference': ref, 'relative': rel},
        title={"text": title, 'font': {'size': 20,
                                       'color': 'darkgrey', 'family': 'Droid Serif'}},
        number={'prefix': "$", 'font': {'size': 30,
                                        'color': 'black', 'family': 'Droid Serif'}},
        domain={'x': [0, 1], 'y': [0, 1]}))

    fig.update_layout({'autosize': True, 'height': 90,
                       'width': 150, 'paper_bgcolor': col})

    return fig


def bar_indicator(value=10, ref=5, title=''):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        delta={'reference': ref, 'relative': True},
        title={'text': title},
        gauge={
            'shape': "angular",
            "axis": {
                "range": [0, ref+ref/5],
                "tickwidth": 1,
                "tickcolor": "white"
            },
            "bgcolor": "white",
            "borderwidth": 2,

            "threshold": {
                "line": {
                    "color": "black",
                    "width": 4
                },
                "thickness": 0.75,
                "value": ref
            }
        },
        domain={'x': [0, 1], 'y': [0, 1]}))

    fig.update_layout({'autosize': True, 'height': 350, 'width': 400})

    return fig
