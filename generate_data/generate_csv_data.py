#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import sys
import argparse
from typing import Tuple
import pandas as pd
from faker import Faker
from common_vars import DATA_DIRECTORY, FLIGHTS, BUS, TRAIN
from common_funcs import get_verbose_logger, get_s3_client, write_object_to_s3, get_ddb_client, write_ddb_object, get_logger, \
    get_verbose, get_on_aws, get_on_ddb, get_transportation_type, get_overwrite, get_aws_profile, get_bucket, transport_in_list, generate_json_file

transportation_type_list = [FLIGHTS, BUS, TRAIN]


def populate_df(generation_number: int, transportation_type: str) -> Tuple[pd.DataFrame, str]:
    # TODO: Improve this function so that it can generate better random data
    fake = Faker()
    type_number = f'{transportation_type}_number'
    letter = type_number[0].upper()
    rows = []
    header = [type_number, 'from_Country', 'to_Country',
              'from_city', 'to_city',
              'from_date', 'to_date',
              'departure', 'arrival',
              'economy', 'eusiness', 'first_class']

    for _ in range(generation_number):
        data = {}
        data['from_country'] = fake.country()
        data['to_country'] = fake.country()
        data['from_city'] = fake.city()
        data['to_city'] = fake.city()
        data['from_date'] = fake.date_this_decade().strftime('%Y-%m-%d')
        data['to_date'] = fake.date_this_decade().strftime('%Y-%m-%d')
        data[type_number] = str(fake.numerify(text=f'{letter}######'))
        data['departure'] = fake.time(pattern='%H:%M')
        data['arrival'] = fake.time(pattern='%H:%M')
        data['economy'] = fake.random_int(min=100, max=1000, step=100)
        data['business'] = fake.random_int(
            min=1000, max=2000, step=100)
        data['first_class'] = fake.random_int(
            min=2000, max=3000, step=100)

        rows.append([data[type_number], data['from_country'], data['to_country'],
                     data['from_city'], data['to_city'], data['from_date'], data['to_date'],
                     data['departure'], data['arrival'], data['economy'], data['business'], data['first_class']])
    return pd.DataFrame(rows, columns=header), type_number


def generate_json(df: pd.DataFrame, type_number: str):
    json = df.set_index(type_number).to_json(
        orient='index')
    return json


def generate_csv_data(generation_number: int, transportation_type: str, aws_creds: str,
                      on_aws: bool, bucket: str, on_ddb: bool, overwrite: bool, json: bool) -> bool:
    # TODO: Adjust the overwrite logic so it can work dynamically

    if overwrite:
        try:
            if not transport_in_list:
                return False

            df, type_number = populate_df(
                generation_number, transportation_type)

            if not on_aws and not on_ddb:
                if not os.path.exists(DATA_DIRECTORY):
                    os.makedirs(DATA_DIRECTORY)
                    verboseprint(f'Directory is: {DATA_DIRECTORY}')
                    log(f'Directory is: {DATA_DIRECTORY}', 'INFO', logger)
                df.to_csv(os.path.join(DATA_DIRECTORY,
                                       f'{transportation_type}.csv'), index=False)
                if json:
                    with open(os.path.join(DATA_DIRECTORY, f'{transportation_type}.json'), 'w', encoding='utf-8') as f:
                        f.write(generate_json(df, type_number))
            else:
                if on_aws:
                    write_object_to_s3(bucket, f'{transportation_type}.csv', df.to_csv(
                        index=False), get_s3_client(aws_creds))
                if on_ddb:
                    write_ddb_object(get_ddb_client(aws_creds),
                                     f'webapp-{transportation_type}', df)
                if json:
                    write_object_to_s3(
                        bucket, f'{transportation_type}.json', generate_json(df, type_number),  get_s3_client(aws_creds))

        except Exception as error:
            verboseprint(
                f'Error in generating the {transportation_type}.csv - {error}')
            log(
                f'Error in generating the {transportation_type}.csv - {error}', 'ERROR', logger)
            return False
    else:
        verboseprint(
            f'Error in generating the {transportation_type}.csv - Overwrite is not enabled')
        log(f'Error in generating the {transportation_type}.csv - Overwrite is not enabled', 'ERROR', logger)
        return False
    return True


def main():
    global verboseprint
    global log
    global logger
    (generation_number,
     transportation_type,
     aws_creds,
     on_aws,
     bucket,
     on_ddb,
     json,
     overwrite,
     verbose,
     logger) = check_args(sys.argv[1:])

    verboseprint, log, logger = get_verbose_logger(verbose, logger)

    if int(generation_number) < 1:
        return False

    verboseprint((f' ARGUMENTS\n'
                  f' generation_number: {generation_number}\n'
                  f' transportation_type: {transportation_type}\n'
                  f' on_aws: {on_aws}\n'
                  f' bucket: {bucket}\n'
                  f' on_ddb: {on_ddb}\n'
                  f' json_file: {json}\n'
                  f' overwrite: {overwrite}\n'
                  f' verbose: {verbose}\n'
                  f' logger: {logger}\n'))

    for transport_type in transportation_type:
        if generate_csv_data(int(generation_number), transport_type, aws_creds,
                             on_aws, bucket, on_ddb, overwrite, json):

            verboseprint(
                f'Successfully generated the {transport_type}.csv file')
            if json:
                verboseprint(
                    f'Successfully generated the json-{transport_type}.json file')
            log(f'Successfully generated the {transport_type}.csv file',
                'INFO', logger)
            if json:
                log(f'Successfully generated the json-{transport_type}.json file',
                    'INFO', logger)
        else:
            verboseprint(
                f'Failed to generate the {transport_type}.csv file')
            if json:
                verboseprint(
                    f'Failed to generate the json-{transport_type}.json file')
            log(f'Failed to generate the {transport_type}.csv file',
                'ERROR', logger)
            if json:
                log(f'Failed to generate the json-{transport_type}.json file',
                    'ERROR', logger)
            return False
    return True


def check_args(args=None) -> Tuple[str, str, str, bool, str, bool, bool, bool, bool, bool]:
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate csv file/s")

    parser.add_argument(
        "-g", "--generation_number",
        help="Enter how many rows of data you want to generate",
        required=True,
        default='default')

    get_transportation_type(parser)
    get_aws_profile(parser)
    get_on_aws(parser)
    get_bucket(parser)
    get_on_ddb(parser)
    generate_json_file(parser)
    get_overwrite(parser)
    get_verbose(parser)
    get_logger(parser)

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.generation_number,
            cmd_line_args.transportation_type,
            cmd_line_args.aws_profile,
            cmd_line_args.on_aws,
            cmd_line_args.bucket,
            cmd_line_args.on_ddb,
            cmd_line_args.json,
            cmd_line_args.overwrite,
            cmd_line_args.verbose,
            cmd_line_args.logger
            )


if __name__ == '__main__':
    main()
