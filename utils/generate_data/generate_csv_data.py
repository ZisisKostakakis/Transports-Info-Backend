#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import sys
import os
import pandas as pd
from faker import Faker
from common_vars import DATA_DIRECTORY, FLIGHTS, BUS, TRAIN, get_verbose
from common_funcs import get_s3_client, write_object_to_s3, get_ddb_client,\
    write_ddb_object

transportation_type_list = [FLIGHTS, BUS, TRAIN]


def populate_df(generation_number, transportation_type):
    fake = Faker()
    type_number = f'{transportation_type}_number'
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
        data['flight_number'] = str(fake.numerify(text='F###'))
        data['departure'] = fake.time(pattern='%H:%M')
        data['arrival'] = fake.time(pattern='%H:%M')
        data['economy'] = fake.random_int(min=100, max=1000, step=100)
        data['business'] = fake.random_int(
            min=1000, max=2000, step=100)
        data['first_class'] = fake.random_int(
            min=2000, max=3000, step=100)

        rows.append([data['flight_number'], data['from_country'], data['to_country'],
                     data['from_city'], data['to_city'], data['from_date'], data['to_date'],
                     data['departure'], data['arrival'], data['economy'], data['business'], data['first_class']])
    return pd.DataFrame(rows, columns=header)


def transport_in_list(value):
    if value not in transportation_type_list:
        msg = f'Error in {value} - Invalid transportation type'
        verboseprint(msg)
        return False
    return True


def generate_csv_data(generation_number, transportation_type, aws_creds, on_aws, bucket, on_ddb, overwrite, verbose):
    if overwrite:
        try:
            if not transport_in_list:
                return False

            df = populate_df(generation_number, transportation_type)

            if not on_aws and not on_ddb:
                if not os.path.exists(DATA_DIRECTORY):
                    os.makedirs(DATA_DIRECTORY)
                    verboseprint(f'Directory is: {DATA_DIRECTORY}')
                df.to_csv(os.path.join(DATA_DIRECTORY,
                          f'{transportation_type}.csv'), index=False)
            else:
                if on_aws:
                    write_object_to_s3(bucket, f'{transportation_type}.csv', df.to_csv(
                        index=False), get_s3_client(aws_creds))
                if on_ddb:
                    write_ddb_object(get_ddb_client(aws_creds),
                                     f'webapp-{transportation_type}', df)
        except Exception as e:
            verboseprint(
                f'Error in generating the {transportation_type} - {e}')
            return False
    else:
        verboseprint(
            f'Error in generating the {transportation_type} - Overwrite is not enabled')
        return False
    return True


def main():
    global verboseprint
    (generation_number,
     transportation_type,
     aws_creds,
     on_aws,
     bucket,
     on_ddb,
     overwrite,
     verbose) = check_args(sys.argv[1:])
    verboseprint = print if verbose else lambda *a, **k: None

    verboseprint((f' ARGUMENTS\n'
                  f' generation_number: {generation_number}\n'
                  f' transportation_type: {transportation_type}\n'
                  f' on_aws: {on_aws}\n'
                  f' bucket: {bucket}\n'
                  f' on_ddb: {on_ddb}\n'
                  f' overwrite: {overwrite}\n'
                  f' verbose: {verbose}\n'))

    if generate_csv_data(int(generation_number), transportation_type, aws_creds,
                         on_aws, bucket, on_ddb, overwrite, verbose):
        verboseprint(
            f'Successfully generated the {transportation_type}.csv file')
    else:
        verboseprint(f'Failed to generate the {transportation_type}.csv file')

    return 0


def check_args(args=None):
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate flights.csv file")
    parser.add_argument(
        "-g", "--generation_number",
        help="Enter how many rows of data you want to generate",
        required=True,
        default='default')

    parser.add_argument(
        "-type", "--transportation_type",
        help="Enter the transportation type. Valid types are: {flights, bus, train}",
        required=True,
        default='flights'
    )

    parser.add_argument(
        "-u", "--aws_profile",
        help="Enter the AWS profile name. Default is 'webapp'",
        required=False,
        default='webapp',
    )

    parser.add_argument(
        "-onaws", "--on_aws",
        help="Write the file to AWS S3",
        required=False,
        default=False,
        action='store_true'
    )

    parser.add_argument(
        "-b", "--bucket",
        help="Enter the bucket name. Default is 'web-app-python'",
        required=False,
        default='web-app-python',
    )

    parser.add_argument(
        "-onddb", "--on_ddb",
        help="Write the file to AWS DynamoDB.",
        required=False,
        default=False,
        action='store_true'
    )

    parser.add_argument(
        "-o", "--overwrite",
        help="Overwrite the existing file",
        required=False,
        default=False,
        action='store_true'
    )

    get_verbose(parser)

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.generation_number,
            cmd_line_args.transportation_type,
            cmd_line_args.aws_profile,
            cmd_line_args.on_aws,
            cmd_line_args.bucket,
            cmd_line_args.on_ddb,
            cmd_line_args.overwrite,
            cmd_line_args.verbose
            )


if __name__ == '__main__':
    main()
