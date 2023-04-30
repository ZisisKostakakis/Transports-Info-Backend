#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json
import os
import sys
from typing import Tuple
import pandas as pd
from common_vars import DATA_DIRECTORY
from common_funcs import get_verbose_logger, get_verbose, get_transportation_type, get_on_aws, get_aws_profile, get_bucket, \
    get_s3_client, check_if_object_exists_in_s3, read_object_from_s3, transport_in_list, get_logger, generate_json_file


def check_local_exist(transportation_type: str) -> bool:
    if os.path.exists(f'{DATA_DIRECTORY}{transportation_type}.csv'):
        return True
    return False


def handle_df(obj: str) -> pd.DataFrame:
    rows = obj.split('\n')
    df = pd.DataFrame(rows)
    df = df[0].str.split(',', expand=True)
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    return df


def get_csv_data(transportation_type: str, aws_profile: str,
                 on_aws: bool, bucket: str, verboseprint, log, logger) -> Tuple[bool, pd.DataFrame]:
    df = pd.DataFrame()
    try:
        if not transport_in_list:
            return False, df
        if on_aws:
            s3_client = get_s3_client(aws_profile)
            if check_if_object_exists_in_s3(bucket, f'{transportation_type}.csv', s3_client=s3_client):
                verboseprint(
                    f'Object {transportation_type}.csv exists in S3, retrieving from S3...')
                log(f'Object {transportation_type}.csv exists in S3, retrieving from S3...', 'INFO', logger)
                obj = read_object_from_s3(
                    bucket, f'{transportation_type}.csv', s3_client)
                df = handle_df(obj)
                # verboseprint(df)
                return True, df
        elif check_local_exist(transportation_type):
            verboseprint(
                f'Object {transportation_type}.csv exists locally, retrieving from local...')
            log(f'Object {transportation_type}.csv exists locally, retrieving from local...', 'INFO', logger)
            df = pd.read_csv(
                f'{DATA_DIRECTORY}{transportation_type}.csv', encoding='utf-8')
            verboseprint(df)
            return True, df
        return False, df
    except Exception as e:
        verboseprint(f'Error in get_csv_data() - {e}')
        log(f'Error in get_csv_data() - {e}', 'ERROR', logger)
        return False, df


def get_json_data(transportation_type: str, aws_profile: str,
                  on_aws: bool, bucket: str, verboseprint, log, logger) -> Tuple[bool, dict]:

    json_data = {}
    try:
        if not transport_in_list:
            return False, json_data
        if on_aws:
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


def main():
    global verboseprint
    global log
    global logger
    (transportation_type,
     aws_profile,
     on_aws,
     bucket,
     json,
     verbose,
     logger) = check_args(sys.argv[1:])

    verboseprint, log, logger = get_verbose_logger(verbose, logger)

    verboseprint((f' transportation_type: {transportation_type}\n'
                  f' aws_profile: {aws_profile}\n'
                  f' on_aws: {on_aws}\n'
                  f' bucket: {bucket}\n'
                  f' verbose: {verbose}\n'
                  f'logger: {logger}'))

    for transportation_type in transportation_type:
        if get_csv_data(transportation_type, aws_profile, on_aws, bucket, verboseprint, log, logger)[0]:
            verboseprint(f'{transportation_type}.csv has successful retrieved')
            log(f'{transportation_type}.csv has successful retrieved', 'INFO', logger)
        else:
            verboseprint(f'{transportation_type}.csv has failed to retrieve')
            log(f'{transportation_type}.csv has failed to retrieve', 'ERROR', logger)
            return False
        if json:
            get_json_data(transportation_type, aws_profile,
                          on_aws, bucket, verboseprint, log, logger)
            verboseprint(
                f'{transportation_type}.json has successful retrieved')
            log(f'{transportation_type}.json has successful retrieved', 'INFO', logger)
        else:
            verboseprint(f'{transportation_type}.json has failed to retrieve')
            log(f'{transportation_type}.json has failed to retrieve', 'ERROR', logger)
            return False

    return True


def check_args(args=None) -> Tuple[str, str, bool, str, bool, bool, bool]:
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate flights.csv file")

    get_transportation_type(parser)
    get_aws_profile(parser)
    get_on_aws(parser)
    get_bucket(parser)
    generate_json_file(parser)
    get_verbose(parser)
    get_logger(parser)

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.transportation_type,
            cmd_line_args.aws_profile,
            cmd_line_args.on_aws,
            cmd_line_args.bucket,
            cmd_line_args.json,
            cmd_line_args.verbose,
            cmd_line_args.logger
            )


if __name__ == '__main__':
    main()
