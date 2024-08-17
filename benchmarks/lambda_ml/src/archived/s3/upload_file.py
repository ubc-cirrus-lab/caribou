import logging
import boto3
from botocore.exceptions import ClientError


def upload_file(dest_bucket_name, dest_file_key, upload_path):
    """Add an file to an Amazon S3 bucket

    The src_data argument must be of type bytes or a string that references
    a file specification.

    :param dest_bucket_name: string
    :param dest_file_key: string
    :param upload_path: string
    :return: True if file was added to dest_bucket/dest_file_key, otherwise
    False
    """

    # upload_file
    s3 = boto3.client('s3')
    try:
        s3.upload_file(upload_path, dest_bucket_name, dest_file_key) 
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(e)
        return False
    return True