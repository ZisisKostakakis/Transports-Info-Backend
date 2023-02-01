from common_vars import AWS_CREDS_DIR, AWS_CREDS_FILE
import boto3


def get_aws_creds():
    """Get AWS credentials from ~/.aws/credentials file"""
    with open(f'{AWS_CREDS_DIR}{AWS_CREDS_FILE}', 'r') as f:
        lines = f.readlines()
    aws_access_key_id = lines[1].split('=')[1].strip()
    aws_secret_access_key = lines[2].split('=')[1].strip()
    return aws_access_key_id, aws_secret_access_key


def get_boto3_session():
    """Get boto3 session"""
    aws_access_key_id, aws_secret_access_key = get_aws_creds()
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='eu-west-2'
    )
    return session


def get_s3_client():
    """Get boto3 s3 client"""
    session = get_boto3_session()
    s3 = session.client('s3')
    return s3


def read_object_from_s3(bucket_name, object_name):
    """Read object from S3 bucket"""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket_name, Key=object_name)
    return obj


def write_object_to_s3(bucket_name, object_name, data):
    """Write object to S3 bucket"""
    s3 = get_s3_client()
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=data)


def get_ddb_client():
    """Get boto3 DynamoDB client"""
    session = get_boto3_session()
    ddb = session.client('dynamodb')
    return ddb


def get_ddb_client():
    """Get boto3 DynamoDB client"""
    session = get_boto3_session()
    ddb = session.client('dynamodb')
    return ddb


def get_ddb_object(ddb, table_name, key):
    """Get DynamoDB object"""
    response = ddb.get_item(
        TableName=table_name,
        Key=key
    )
    return response


def write_ddb_object(ddb, table_name, item):
    """Write DynamoDB object"""
    response = ddb.put_item(
        TableName=table_name,
        Item=item
    )
    return response


def write_object_to_both_s3_and_ddb(bucket_name, object_name, data, table_name, key):
    """Write object to S3 and DynamoDB"""
    write_object_to_s3(bucket_name, object_name, data)
    ddb = get_ddb_client()
    write_ddb_object(ddb, table_name, key)


def check_if_object_exists_in_s3(bucket_name, object_name):
    """Check if object exists in S3"""
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket_name, Key=object_name)
        return True
    except:
        return False


def check_if_object_exists_in_ddb(ddb, table_name, key):
    """Check if object exists in DynamoDB"""
    try:
        get_ddb_object(ddb, table_name, key)
        return True
    except:
        return False


def check_if_object_exists_in_both_s3_and_ddb(bucket_name, object_name, table_name, key):
    """Check if object exists in S3 and DynamoDB"""
    s3 = get_s3_client()
    ddb = get_ddb_client()
    try:
        s3.head_object(Bucket=bucket_name, Key=object_name)
        get_ddb_object(ddb, table_name, key)
        return True
    except:
        return False


def get_list_of_buckets():
    """Get list of buckets"""
    s3 = get_s3_client()
    response = s3.list_buckets()
    return response


def get_list_of_objects_s3(bucket_name):
    """Get list of objects in bucket"""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket_name)
    return response


def get_list_of_objects_ddb(ddb, table_name):
    """Get list of objects in DynamoDB table"""
    response = ddb.scan(TableName=table_name)
    return response


def get_object_from_s3(bucket_name, object_name):
    """Get object from S3"""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket_name, Key=object_name)
    return obj


def write_object_to_s3(bucket_name, object_name, data):
    """Write object to S3"""
    s3 = get_s3_client()
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=data)


def delete_object_from_s3(bucket_name, object_name):
    """Delete object from S3"""
    s3 = get_s3_client()
    s3.delete_object(Bucket=bucket_name, Key=object_name)
