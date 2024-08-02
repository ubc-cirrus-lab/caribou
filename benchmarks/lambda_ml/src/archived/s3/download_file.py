import logging
import boto3
from botocore.exceptions import ClientError


def download_file(dest_bucket_name, dest_file_key):
    """Fetch an file to an Amazon S3 bucket

    The src_data argument must be of type bytes or a string that references
    a file specification.

    :param dest_bucket_name: string
    :param dest_file_key: string
    :return: download path if get the file successfully, otherwise
    False
    """  

    # get the file
    s3 = boto3.client('s3')
    download_path = '/tmp/{}'.format(dest_file_key)
    try:
        s3.download_file(dest_bucket_name, dest_file_key, download_path)    
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(e)
        return False

    return download_path