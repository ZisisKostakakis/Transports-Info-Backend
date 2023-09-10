#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json
import os
from typing import Tuple
import logging
import boto3
import pandas as pd
from common_vars import transportation_type_list, DATA_DIRECTORY
from mypy_boto3_s3.client import S3Client
from mypy_boto3_dynamodb.client import DynamoDBClient
from boto3.session import Session

def get_verbose_logger(verbose: bool, logger_arg: bool):
    verboseprint = print if verbose else lambda *a, **k: None
    logger = get_logger_instance() if logger_arg else None
    log = log_msg if logger else lambda *a, **k: None
    return verboseprint, log, logger


def get_logger_instance() -> logging.Logger:
    logging.basicConfig(filename='webapp.log', level=logging.INFO, filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    return logging.getLogger(__name__)


def log_msg(msg: str, level: str, logger) -> None:
    if level == 'INFO':
        logger.info(msg)
    elif level == 'WARNING':
        logger.warning(msg)
    elif level == 'ERROR':
        logger.error(msg)
    elif level == 'CRITICAL':
        logger.critical(msg)


def get_logger(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-lg", "--logger",
        help="Enable logging",
        required=False,
        default=False,
        action='store_true')


def get_verbose(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-v", "--verbose",
        help="Enable verbosity",
        required=False,
        default=False,
        action='store_true')


def generate_json_file(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-j", "--json",
        help="Generate JSON file for S3",
        required=False,
        default=False,
        action='store_true')


def get_transportation_type(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-type", "--transportation_type",
        nargs='*',
        help="Enter the transportation type. Valid types are: {flights, bus, train}",
        required=True,
        default=[]
    )


def get_aws_profile(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-u", "--aws_profile",
        help="Enter the AWS profile name. Default is 'webapp'",
        required=False,
        default='webapp',
    )


def get_on_aws(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-onaws", "--on_aws",
        help="Write the file to AWS S3",
        required=False,
        default=False,
        action='store_true'
    )


def get_bucket(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-b", "--bucket",
        help="Enter the bucket name. Default is 'web-app-python'",
        required=False,
        default='web-app-python',
    )


def get_on_ddb(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-onddb", "--on_ddb",
        help="Write the file to AWS DynamoDB.",
        required=False,
        default=False,
        action='store_true'
    )


def get_overwrite(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-o", "--overwrite",
        help="Overwrite the existing file",
        required=False,
        default=False,
        action='store_true'
    )


def get_aws_creds(aws_creds: str) -> Tuple[str, str]:
    """Get AWS credentials from ~/.aws/credentials file"""
    session = boto3.Session(profile_name=aws_creds)
    credentials = session.get_credentials()
    aws_access_key_id = credentials.access_key
    aws_secret_access_key = credentials.secret_key

    return aws_access_key_id, aws_secret_access_key


def get_boto3_session(aws_creds: str) -> Session:
    """Get boto3 session"""
    aws_access_key_id, aws_secret_access_key = get_aws_creds(
        aws_creds=aws_creds)
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='eu-west-2'
    )
    return session


def get_s3_client(aws_creds: str) -> S3Client:
    """Get boto3 s3 client"""
    session = get_boto3_session(aws_creds=aws_creds)
    s3 = session.client('s3')
    return s3


def read_object_from_s3(bucket_name: str, object_name: str, s3_client) -> str:
    """Read object from S3 bucket"""
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_name).get(
        'Body').read().decode('utf-8')
    return obj


def write_object_to_s3(bucket_name: str, object_name: str, data: str, s3_client) -> None:
    """Write object to S3 bucket"""
    s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)


def get_ddb_client(aws_creds: str) -> DynamoDBClient:
    """Get boto3 DynamoDB client"""
    session = get_boto3_session(aws_creds=aws_creds)
    ddb = session.client('dynamodb')
    return ddb


def get_ddb_object(ddb_client, table_name: str, key: dict) -> dict:
    """Get DynamoDB object"""
    response = ddb_client.get_item(
        TableName=table_name,
        Key=key
    )
    return response


def write_ddb_object(ddb_client, table_name: str, data: pd.DataFrame) -> None:
    for row in data.iterrows():
        item = {col: {'S': str(value)} for col, value in row[1].items()}
        ddb_client.put_item(TableName=table_name, Item=item)


def write_object_to_both_s3_and_ddb(bucket_name: str, object_name: str, data: str, table_name: str, key: pd.DataFrame, s3_client, ddb_client) -> None:
    """Write object to S3 and DynamoDB"""
    write_object_to_s3(bucket_name, object_name, data, s3_client)
    write_ddb_object(ddb_client, table_name, key)


def check_if_object_exists_in_s3(bucket_name: str, object_name: str, s3_client) -> bool:
    """Check if object exists in S3"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        return True
    except Exception as e:
        print(e)
        return False


def check_if_object_exists_in_ddb(ddb_client, table_name: str, key: dict) -> bool:
    """Check if object exists in DynamoDB"""
    try:
        get_ddb_object(ddb_client, table_name, key)
        return True
    except Exception as e:
        print(e)
        return False


def check_if_object_exists_in_both_s3_and_ddb(bucket_name: str, object_name: str, table_name: str, key: dict, s3_client, ddb_client) -> bool:
    """Check if object exists in S3 and DynamoDB"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        get_ddb_object(ddb_client, table_name, key)
        return True
    except Exception as e:
        print(e)
        return False


def get_list_of_buckets(s3_client) -> dict:
    """Get list of buckets"""
    response = s3_client.list_buckets()
    return response


def get_list_of_objects_s3(bucket_name: str, s3_client) -> dict:
    """Get list of objects in bucket"""
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    return response


def get_list_of_objects_ddb(ddb_client, table_name: str) -> dict:
    """Get list of objects in DynamoDB table"""
    response = ddb_client.scan(TableName=table_name)
    return response


def delete_object_from_s3(bucket_name: str, object_name: str, s3_client) -> None:
    """Delete object from S3"""
    s3_client.delete_object(Bucket=bucket_name, Key=object_name)


def transport_in_list(value: str) -> bool:
    if value not in transportation_type_list:
        msg = f'Error in {value} - Invalid transportation type'
        print(msg)
        return False
    return True


def get_json_data(transportation_type: str, aws_profile: str, s3_client,
                  on_aws: bool, bucket: str, verboseprint, log, logger) -> Tuple[bool, dict]:

    json_data = {}
    try:
        if not transport_in_list:
            return False, json_data
        if on_aws:
            if aws_profile == '':
                s3_client = get_s3_client(aws_profile)
            if check_if_object_exists_in_s3(bucket, f'{transportation_type}.json', s3_client=s3_client):
                verboseprint(
                    f'Object {transportation_type}.json exists in S3, retrieving from S3...')
                log(f'Object {transportation_type}.json exists in S3, retrieving from S3...', 'INFO', logger)
                obj = read_object_from_s3(
                    bucket, f'{transportation_type}.json', s3_client)
                json_data = json.loads(obj)
                verboseprint(json_data)
                return True, json_data
        elif check_local_exist(transportation_type):
            verboseprint(
                f'Object {transportation_type}.json exists locally, retrieving from local...')
            log(f'Object {transportation_type}.json exists locally, retrieving from local...', 'INFO', logger)
            with open(f'{DATA_DIRECTORY}{transportation_type}.json', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            verboseprint(json_data)
            return True, json_data
        return False, json_data
    except Exception as e:
        verboseprint(f'Error in get_json_data() - {e}')
        log(f'Error in get_json_data() - {e}', 'ERROR', logger)
        return False, json_data


def check_local_exist(transportation_type: str) -> bool:
    if os.path.exists(f'{DATA_DIRECTORY}{transportation_type}.csv'):
        return True
    return False
