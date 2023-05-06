#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Run this app with `python app.py` and
# visit http://127.0.0.1:5000/ in your web browser.
import json
# import os
from flask import Flask
from dotenv import load_dotenv
from common_vars import FLIGHTS, BUS, TRAIN
# from generate_csv_data import generate_csv_data
# from get_data import get_json_data
from common_funcs import get_verbose_logger
load_dotenv()
server = Flask(__name__)
global verboseprint
global log
global logger


@server.route('/')
def home():
    return 'Welcome to the transport data API!'


@server.route('/<data_type>')
def get_transport_data(data_type):
    verboseprint, log, logger = get_verbose_logger(True, False)
    if data_type == 'flights':
        return json.dumps(get_transport_list(FLIGHTS, verboseprint, log, logger), indent=4, sort_keys=True)
    if data_type == 'bus':
        return json.dumps(get_transport_list(BUS, verboseprint, log, logger), indent=4, sort_keys=True)
    if data_type == 'train':
        return json.dumps(get_transport_list(TRAIN, verboseprint, log, logger), indent=4, sort_keys=True)
    return 'Invalid data type'


def get_transport_list(transportation_type, verboseprint, log, logger):
    # ttype = str(transportation_type)
    # try:
    #     success, transport_list = get_json_data(ttype, aws_profile=str(os.environ.get('AWS_PROFILE')), on_aws=True, bucket=str(
    #         os.environ.get('AWS_BUCKET')), verboseprint=verboseprint, log=log, logger=logger)

    #     if success:
    #         return transport_list

    #     generate_csv_data(50, ttype, aws_creds=str(os.environ.get('AWS_PROFILE')),
    #                       on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), on_ddb=False, overwrite=True, json=True)
    #     return get_json_data(ttype, aws_profile=str(os.environ.get('AWS_PROFILE')), on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), verboseprint=verboseprint, log=log, logger=logger)[1]

    # except Exception:
    #     generate_csv_data(50, ttype, aws_creds=str(os.environ.get('AWS_PROFILE')),
    #                       on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), on_ddb=False, overwrite=True, json=True)
    #     return get_json_data(ttype, aws_profile=str(os.environ.get('AWS_PROFILE')), on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), verboseprint=verboseprint, log=log, logger=logger)[1]
    return json.dumps('Unused')


if __name__ == '__main__':
    server.run(debug=False, port=5000, host='0.0.0.0')
