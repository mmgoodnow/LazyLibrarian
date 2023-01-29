import logging

from io import StringIO
import pandas as pd
import requests
from http.client import responses
from dash import Dash, html, dcc, Input, Output
import plotly.express as px

TELEMETRY_SERVER = 'http://localhost:9174'
intervals = ['Day', 'Hour', 'Week', 'Month']

time_dropdown = dcc.Dropdown(options=intervals, value='Day')




def server_request(url: str) -> (str, bool):
    logger = logging.getLogger(__name__)
    proxies = {}
    timeout = 5
    headers = {'User-Agent': 'LazyLibrarian'}
    if proxies:
        payload = {"timeout": timeout, "proxies": proxies}
    else:
        payload = {"timeout": timeout}
    try:
        logger.debug(f'GET {url}')
        r = requests.get(url, verify=False, params=payload, headers=headers)
    except requests.exceptions.Timeout as e:
        logger.error("_send_url: Timeout sending telemetry %s" % url)
        return "Timeout %s" % str(e), False
    except Exception as e:
        return "Exception %s: %s" % (type(e).__name__, str(e)), False

    if str(r.status_code).startswith('2'):  # (200 OK etc)
        return r.text, True  # Success
    if r.status_code in responses:
        msg = responses[r.status_code]
    else:
        msg = r.text
    return "Response status %s: %s" % (r.status_code, msg), False


def get_timeline_telemetry(name: str, granularity: str) -> (str, bool):
    return server_request(f'{TELEMETRY_SERVER}/csv/{name}/{granularity}')


def get_stats_telemetry(name: str) -> (str, bool):
    return server_request(f'{TELEMETRY_SERVER}/stats/{name}')


app = Dash()
app.layout = html.Div(children=[
    html.Div(children=[
        html.H1(children='LazyLibrarian usage statistics'),
        html.H2(children='OS distribution'),
        #dcc.Graph(id='os_distribution', figure=os_distribution),
    ]),
    html.Div(children=[
        html.H2(children='Reports over time'),
        time_dropdown,
        dcc.Graph(id='unique_over_time')
    ])
])


@app.callback(
    Output(component_id='unique_over_time', component_property='figure'),
    Input(component_id=time_dropdown, component_property='value')
)
def update_timeline(selected_granularity):
    csv_data, ok = get_timeline_telemetry('servers', selected_granularity)
    if ok:
        dataframe = pd.read_csv(StringIO(csv_data))
        line_fig = px.line(dataframe,
                           x='date', y='reports',
                           title=f'LazyLibrarian reports per {selected_granularity}')
        return line_fig
    else:
        return 'Oops'


app.run_server(debug=True, use_reloader=True)
