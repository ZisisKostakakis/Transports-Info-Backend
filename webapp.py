#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Run this app with `python app.py` and
# visit http://127.0.0.1:5000/ in your web browser.
import os
from flask import Flask
from dotenv import load_dotenv
from common_vars import FLIGHTS, BUS, TRAIN
from generate_csv_data import generate_csv_data
from get_csv_data import get_csv_data
from common_funcs import get_verbose_logger
load_dotenv()
app = Flask(__name__)

@app.route('/<data_type>')
def get_transport_data(data_type):
    if data_type == 'flights':
        return get_transport_list(FLIGHTS).to_json()
    if data_type == 'bus':
        return get_transport_list(BUS).to_json()
    if data_type == 'train':
        return get_transport_list(TRAIN).to_json()
    return 'Invalid data type'

def get_transport_list(transportation_type):
    ttype = str(transportation_type)
    try:
        success, transport_list = get_csv_data(
            ttype, aws_profile=str(os.environ.get('AWS_PROFILE')), on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), verboseprint=verboseprint, log=log, logger=logger)
        if success:
            return transport_list

        generate_csv_data(50,ttype, aws_creds=str(os.environ.get('AWS_PROFILE')),
                                 on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), on_ddb=False, overwrite=True)
        return get_csv_data(ttype, aws_profile=str(os.environ.get('AWS_PROFILE')), on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), verboseprint=verboseprint, log=log, logger=logger)[1]

    except Exception:
        generate_csv_data(50,ttype, aws_creds=str(os.environ.get('AWS_PROFILE')),
                                 on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), on_ddb=False, overwrite=True)
        return get_csv_data(ttype, aws_profile=str(os.environ.get('AWS_PROFILE')), on_aws=True, bucket=str(os.environ.get('AWS_BUCKET')), verboseprint=verboseprint, log=log, logger=logger)[1]

if __name__ == '__main__':
    global verboseprint
    global log
    global logger
    verboseprint, log, logger = get_verbose_logger(True, False)
    app.run(debug=False, port=5001, host='0.0.0.0')