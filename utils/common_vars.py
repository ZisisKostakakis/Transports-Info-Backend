#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from pathlib import Path
AWS_CREDS_DIR = f'{Path.home()}/.aws/'
AWS_CREDS_FILE = 'credentials'
USER_DIRECTORY = project_directory = os.path.abspath(os.path.dirname(__file__))
DATA_DIRECTORY = f'{USER_DIRECTORY}/generate_data/'
FLIGHTS = 'flights'
BUS = 'bus'
TRAIN = 'train'


def get_verbose(parser):

    return parser.add_argument(
        "-v", "--verbose",
        help="Enable verbosity",
        required=False,
        default=False,
        action='store_true')
