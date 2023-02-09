#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
from typing import Tuple
import boto3
import pandas as pd
from common_vars import transportation_type_list


def get_verbose(parser: argparse.ArgumentParser):
    return parser.add_argument(
        "-v", "--verbose",
        help="Enable verbosity",
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


def get_boto3_session(aws_creds: str) -> boto3.Session:
    """Get boto3 session"""
    aws_access_key_id, aws_secret_access_key = get_aws_creds(
        aws_creds=aws_creds)
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='eu-west-2'
    )
    return session


def get_s3_client(aws_creds: str):
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


def get_ddb_client(aws_creds: str):
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


def get_list_of_buckets(s3_client):
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
