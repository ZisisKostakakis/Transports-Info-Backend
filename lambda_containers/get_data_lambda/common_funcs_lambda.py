import json
import os
from typing import Tuple
import logging
from common_vars_lambda import transportation_type_list, DATA_DIRECTORY


def get_verbose_logger(verbose: bool, logger_arg: bool):
    verboseprint = print if verbose else lambda *a, **k: None
    logger = get_logger_instance() if logger_arg else None
    log = log_msg if logger else lambda *a, **k: None
    return verboseprint, log, logger


def get_logger_instance() -> logging.Logger:
    logging.basicConfig(filename='webapp.log', level=logging.INFO, filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    return logging.getLogger(__name__)


def log_msg(msg: str, level: str, logger) -> None:
    if level == 'INFO':
        logger.info(msg)
    elif level == 'WARNING':
        logger.warning(msg)
    elif level == 'ERROR':
        logger.error(msg)
    elif level == 'CRITICAL':
        logger.critical(msg)


def get_json_data(transportation_type: str, aws_profile: str, s3_client,
                  on_aws: bool, bucket: str, verboseprint, log, logger) -> Tuple[bool, dict]:

    json_data = {}
    try:
        if not transport_in_list:
            return False, json_data
        if on_aws:
            if check_if_object_exists_in_s3(bucket, f'{transportation_type}.json', s3_client=s3_client):
                verboseprint(
                    f'Object {transportation_type}.json exists in S3, retrieving from S3...')
                obj = read_object_from_s3(
                    bucket, f'{transportation_type}.json', s3_client)
                json_data = json.loads(obj)
                verboseprint(json_data)
                return True, json_data
        elif check_local_exist(transportation_type):
            verboseprint(
                f'Object {transportation_type}.json exists locally, retrieving from local...')
            with open(f'{DATA_DIRECTORY}{transportation_type}.json', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            verboseprint(json_data)
            return True, json_data
        return False, json_data
    except Exception as e:
        verboseprint(f'Error in get_json_data() - {e}')
        return False, json_data


def check_local_exist(transportation_type: str) -> bool:
    if os.path.exists(f'{DATA_DIRECTORY}{transportation_type}.csv'):
        return True
    return False


def check_if_object_exists_in_s3(bucket_name: str, object_name: str, s3_client) -> bool:
    """Check if object exists in S3"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        return True
    except Exception as e:
        print(e)
        return False


def read_object_from_s3(bucket_name: str, object_name: str, s3_client) -> str:
    """Read object from S3 bucket"""
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_name).get(
        'Body').read().decode('utf-8')
    return obj


def transport_in_list(value: str) -> bool:
    if value not in transportation_type_list:
        msg = f'Error in {value} - Invalid transportation type'
        print(msg)
        return False
    return True
