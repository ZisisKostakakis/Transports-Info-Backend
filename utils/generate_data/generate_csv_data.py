#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import argparse
import sys
import os
import pandas as pd
from faker import Faker
from common_vars import USER_DIRECTORY, DATA_DIRECTORY, FLIGHTS, BUS, TRAIN
from common_funcs import get_aws_creds, get_boto3_session, get_s3_client,\
    read_object_from_s3, write_object_to_s3, get_ddb_client, get_ddb_object, write_ddb_object

transportation_type_list = [FLIGHTS, BUS, TRAIN]


def generate_csv_data(generation_number, transportation_type, aws_creds, on_aws, bucket, on_ddb, overwrite, verbose):
    if overwrite:
        try:
            if transportation_type not in transportation_type_list:
                if verbose:
                    msg = 'Error in {} - Invalid transportation type'.format(
                        transportation_type)
                    print(msg)
                return False
            fake = Faker()
            Faker.seed(0)
            if transportation_type == FLIGHTS:
                flight_number = 'Flights_Number'
            elif transportation_type == BUS:
                flight_number = 'Bus_Number'
            elif transportation_type == TRAIN:
                flight_number = 'Train_Number'

            header = [flight_number, 'From_Country', 'To_Country',
                      'From_City', 'To_City',
                      'From_Date', 'To_Date',
                      'Departure', 'Arrival',
                      'Economy', 'Business', 'First_Class']
            rows = []

            for _ in range(generation_number):
                from_country = fake.country()
                to_country = fake.country()
                from_city = fake.city()
                to_city = fake.city()
                from_date = fake.date_this_decade().strftime('%Y-%m-%d')
                to_date = fake.date_this_decade().strftime('%Y-%m-%d')
                flight_number = str(fake.numerify(text='F###'))
                departure = fake.time(pattern='%H:%M')
                arrival = fake.time(pattern='%H:%M')
                economy = fake.random_int(min=100, max=1000, step=100)
                business = fake.random_int(min=1000, max=2000, step=100)
                first_class = fake.random_int(min=2000, max=3000, step=100)

                rows.append([flight_number, from_country, to_country,
                            from_city, to_city, from_date, to_date,
                            departure, arrival, economy, business, first_class])

            directory = os.path.join(
                DATA_DIRECTORY, f'{transportation_type}.csv')
            df = pd.DataFrame(rows, columns=header)

            if not on_aws or not on_ddb:
                if not os.path.exists(DATA_DIRECTORY):
                    os.makedirs(DATA_DIRECTORY)
                if verbose:
                    msg = DATA_DIRECTORY
                    print(msg)
                df.to_csv(directory, index=False)

            if on_aws or on_ddb:
                if on_aws:
                    s3_client = get_s3_client(aws_creds)
                    write_object_to_s3(
                        bucket, f'{transportation_type}.csv', df.to_csv(index=False), s3_client)
                if on_ddb:
                    ddb_client = get_ddb_client(aws_creds)
                    write_ddb_object(
                        ddb_client, f'webapp-{transportation_type}', df)

        except Exception as e:
            if verbose:
                msg = 'Error in generating the {} - {}'.format(
                    transportation_type, e)
                print(msg)
            return False
    else:
        if verbose:
            msg = 'Error in generating the {} - Overwrite is not enabled'.format(
                transportation_type)
            print(msg)
        return False
    return True


def main():
    (generation_number,
     transportation_type,
     aws_creds,
     on_aws,
     bucket,
     on_ddb,
     overwrite,
     verbose) = check_args(sys.argv[1:])

    msg = (f' ARGUMENTS\n'
           f' generation_number: {generation_number}\n'
           f' transportation_type: {transportation_type}\n'
           f' on_aws: {on_aws}\n'
           f' bucket: {bucket}\n'
           f' on_ddb: {on_ddb}\n'
           f' overwrite: {overwrite}\n'
           f' verbose: {verbose}\n')
    if verbose:
        print(msg)
    success = generate_csv_data(
        int(generation_number), transportation_type, aws_creds, on_aws, bucket, on_ddb, overwrite, verbose)
    if success:
        if verbose:
            msg = 'Successfully generated the {}.csv file'.format(
                transportation_type)
            print(msg)
    else:
        if verbose:
            msg = 'Failed to generate the {}.csv file'.format(
                transportation_type)
            print(msg)

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

    parser.add_argument(
        "-v", "--verbose",
        help="Enable verbosity",
        required=False,
        default=False,
        action='store_true')

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
