#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import csv
from common_vars import TRAIN, BUS, FLIGHTS, DATA_DIRECTORY

transportation_type_list = [FLIGHTS, BUS, TRAIN]


def get_csv_data(transportation_type, verbose):
    try:
        if transportation_type not in transportation_type_list:
            if verbose:
                msg = 'Error in {} - Invalid transportation type'.format(
                    transportation_type)
                print(msg)
            return False, pd.DataFrame()
        df = pd.DataFrame()
        df = pd.read_csv(
            f'{DATA_DIRECTORY}{transportation_type}.csv', encoding='utf-8')
        if verbose:
            print(df)
            return True, df
    except Exception as e:
        if verbose:
            msg = 'Error in get_csv_data() - {}'.format(e)
            print(msg)
        return False, df


def main():
    (transportation_type,
     verbose) = check_args(sys.argv[1:])

    msg = (f' transportation_type: {transportation_type}\n'
           f' verbose: {verbose}\n')
    if verbose:
        print(msg)

    success, _ = get_csv_data(transportation_type, verbose)
    if success:
        msg = '{}.csv has successful retrieved'.format(transportation_type)
        print(msg)
    else:
        if verbose:
            msg = 'Failed to retrieve the {}.csv file'.format(
                transportation_type)
            print(msg)
    return


def check_args(args=None):
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate flights.csv file")
    parser.add_argument(
        "-type", "--transportation_type",
        help="Enter the transportation type. Valid types are: {FLIGHTS}, {BUS}, {TRAIN}}",
        required=False,
        default='default')

    parser.add_argument(
        "-v", "--verbose",
        help="Enable verbosity",
        required=False,
        default=False,
        action='store_true')

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.transportation_type,
            cmd_line_args.verbose
            )


if __name__ == '__main__':
    main()
