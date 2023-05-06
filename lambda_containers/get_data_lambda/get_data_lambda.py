#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import boto3
from common_funcs_lambda import get_json_data, get_verbose_logger


def get_data(transportation_type):
    verboseprint, log, _ = get_verbose_logger(True, True)
    s3_client = boto3.client('s3', region_name='eu-west-2')
    tupl = get_json_data(transportation_type=transportation_type, aws_profile='', s3_client=s3_client, on_aws=True,
                         bucket='web-app-python', verboseprint=verboseprint, log=log, logger=True)
    if tupl[0]:
        return tupl[1]
    return None


def lambda_handler(event, context):
    # if 'transportation_type' in event['queryStringParameters']:
    # transportation_type = event['queryStringParameters']['transportation_type']
    if 'transportation_type' in event:
        transportation_type = event['transportation_type']
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('transportation_type not found in event')
        }

    data = get_data(transportation_type)

    if data is None:
        return {
            'statusCode': 400,
            'body': json.dumps('data not found')
        }

    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }
