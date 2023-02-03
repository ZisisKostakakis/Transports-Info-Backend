#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
from common_vars import TRAIN, BUS, FLIGHTS, DATA_DIRECTORY
from common_funcs import get_verbose, get_transportation_type

transportation_type_list = [FLIGHTS, BUS, TRAIN]


def get_csv_data(transportation_type, verbose):
    try:
        if transportation_type not in transportation_type_list:
            verboseprint(
                f'Error in {transportation_type} - Invalid transportation type')
            return False, pd.DataFrame()
        df = pd.DataFrame()
        df = pd.read_csv(
            f'{DATA_DIRECTORY}{transportation_type}.csv', encoding='utf-8')
        verboseprint(df)
        return True, df
    except Exception as e:
        verboseprint(f'Error in get_csv_data() - {e}')
        return False, df


def main():
    global verboseprint
    (transportation_type,
     verbose) = check_args(sys.argv[1:])
    verboseprint = print if verbose else lambda *a, **k: None

    verboseprint((f' transportation_type: {transportation_type}\n'
                  f' verbose: {verbose}\n'))

    if get_csv_data(transportation_type, verbose)[0]:
        verboseprint(f'{transportation_type}.csv has successful retrieved')
    else:
        verboseprint(f'{transportation_type}.csv has failed to retrieve')

    return 0


def check_args(args=None):
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate flights.csv file")

    get_transportation_type(parser)
    get_verbose(parser)

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.transportation_type,
            cmd_line_args.verbose
            )


if __name__ == '__main__':
    main()
