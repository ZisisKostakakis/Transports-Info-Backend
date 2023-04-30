#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys
from typing import Tuple
from common_funcs import get_verbose_logger, get_verbose, get_transportation_type, get_on_aws, get_aws_profile, get_bucket, \
    get_s3_client, read_object_from_s3, get_logger


def get_json_length(transportation_type: str, aws_profile: str,
                    on_aws: bool, bucket: str, verboseprint, log, logger) -> int:
    length = 0
    s3_client = get_s3_client(aws_profile)

    json_object = read_object_from_s3(
        bucket, f'{transportation_type}.json', s3_client)

    print(f'{len(json_object)}')
    return length


def main():
    global verboseprint
    global log
    global logger
    (transportation_type,
     aws_profile,
     on_aws,
     bucket,
     verbose,
     logger) = check_args(sys.argv[1:])

    verboseprint, log, logger = get_verbose_logger(verbose, logger)

    verboseprint((f' transportation_type: {transportation_type}\n'
                  f' aws_profile: {aws_profile}\n'
                  f' on_aws: {on_aws}\n'
                  f' bucket: {bucket}\n'
                  f' verbose: {verbose}\n'
                  f'logger: {logger}'))
    for ttype in transportation_type:
        get_json_length(ttype, aws_profile, on_aws,
                        bucket, verboseprint, log, logger)
    return True


def check_args(args=None) -> Tuple[str, str, bool, str, bool, bool]:
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate flights.csv file")

    get_transportation_type(parser)
    get_aws_profile(parser)
    get_on_aws(parser)
    get_bucket(parser)
    get_verbose(parser)
    get_logger(parser)

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.transportation_type,
            cmd_line_args.aws_profile,
            cmd_line_args.on_aws,
            cmd_line_args.bucket,
            cmd_line_args.verbose,
            cmd_line_args.logger
            )


if __name__ == '__main__':
    main()
