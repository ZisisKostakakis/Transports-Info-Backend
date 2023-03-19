#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Run this app with `python app.py` and
# visit http://127.0.0.1:5000/ in your web browser.
import dash_bootstrap_components as dbc
from flask import Flask
import dash
from dash import html, Output, Input, ctx
from common_vars import FLIGHTS, BUS, TRAIN
from get_csv_data import get_csv_data
from common_funcs import get_verbose_logger

server = Flask(__name__)
app = dash.Dash(__name__, server=server, url_base_pathname='/', suppress_callback_exceptions=True)

app.layout = html.Div([
    html.Button('Flights', id='button-flights', n_clicks=0),
    html.Button('Bus', id='button-bus', n_clicks=0),
    html.Button('Train', id='button-train', n_clicks=0),
    html.Div(id='output')
])

def get_transport_list(transportation_type):
    ttype = str(transportation_type)
    ttype = ttype.replace('[', '').replace(']', '').replace("'", '')
    transport_list = get_csv_data(
        ttype, 'webapp', True, 'web-app-python', verboseprint, log, logger)[1]
    return transport_list

@app.callback(Output('output', 'children'),
              [Input('button-flights', 'n_clicks'),
               Input('button-bus', 'n_clicks'),
               Input('button-train', 'n_clicks')])
def get_transport_info(n_clicks_flights, n_clicks_bus, n_clicks_train):
    flight_clicked = ctx.triggered[0]['prop_id'].split('.')[0] == 'button-flights'
    bus_clicked = ctx.triggered[0]['prop_id'].split('.')[0] == 'button-bus'
    train_clicked = ctx.triggered[0]['prop_id'].split('.')[0] == 'button-train'

    if flight_clicked:
        transport_type = FLIGHTS
    elif bus_clicked:
        transport_type = BUS
    elif train_clicked:
        transport_type = TRAIN
    else:
        transport_type = ''

    data = get_transport_list(transport_type)

    return html.Div([
        html.P(f'Transport type: {transport_type}')
    ]), html.Div([
        dbc.Table.from_dataframe(data, striped=True, bordered=True, hover=True)
    ])

if __name__ == '__main__':
    global verboseprint
    global log
    global logger
    verboseprint, log, logger = get_verbose_logger(True, False)
    server.run(debug=False, port=5001, host='0.0.0.0')