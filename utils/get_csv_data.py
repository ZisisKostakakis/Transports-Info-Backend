#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import sys
import pandas as pd
from common_vars import DATA_DIRECTORY
from common_funcs import get_verbose, get_transportation_type, get_on_aws, get_aws_profile, get_bucket, \
    get_s3_client, check_if_object_exists_in_s3, read_object_from_s3, transport_in_list


def check_local_exist(transportation_type):
    if os.path.exists(f'{DATA_DIRECTORY}{transportation_type}.csv'):
        return True
    return False


def handle_df(obj):
    rows = obj.split('\n')
    df = pd.DataFrame(rows)
    df = df[0].str.split(',', expand=True)
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    return df


def get_csv_data(transportation_type, aws_profile, on_aws, bucket):
    df = pd.DataFrame()
    try:
        if not transport_in_list:
            return False, df
        if on_aws:
            s3_client = get_s3_client(aws_profile)
            if check_if_object_exists_in_s3(bucket, f'{transportation_type}.csv', s3_client=s3_client):
                verboseprint(
                    f'Object {transportation_type}.csv exists in S3, retrieving from S3...')
                obj = read_object_from_s3(
                    bucket, f'{transportation_type}.csv', s3_client)
                df = handle_df(obj)
                verboseprint(df)
                return True, df
        elif check_local_exist(transportation_type):
            verboseprint(
                f'Object {transportation_type}.csv exists locally, retrieving from local...')
            df = pd.read_csv(
                f'{DATA_DIRECTORY}{transportation_type}.csv', encoding='utf-8')
            verboseprint(df)
            return True, df
        return False, df
    except Exception as e:
        verboseprint(f'Error in get_csv_data() - {e}')
        return False, df


def main():
    global verboseprint
    (transportation_type,
     aws_profile,
     on_aws,
     bucket,
     verbose) = check_args(sys.argv[1:])
    verboseprint = print if verbose else lambda *a, **k: None

    verboseprint((f' transportation_type: {transportation_type}\n'
                  f' aws_profile: {aws_profile}\n'
                  f' on_aws: {on_aws}\n'
                  f' bucket: {bucket}\n'
                  f' verbose: {verbose}\n'))

    for transportation_type in transportation_type:
        if get_csv_data(transportation_type, aws_profile, on_aws, bucket)[0]:
            verboseprint(f'{transportation_type}.csv has successful retrieved')
        else:
            verboseprint(f'{transportation_type}.csv has failed to retrieve')


def check_args(args=None):
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate flights.csv file")

    get_transportation_type(parser)
    get_aws_profile(parser)
    get_on_aws(parser)
    get_bucket(parser)
    get_verbose(parser)

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.transportation_type,
            cmd_line_args.aws_profile,
            cmd_line_args.on_aws,
            cmd_line_args.bucket,
            cmd_line_args.verbose
            )


if __name__ == '__main__':
    main()
