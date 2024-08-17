import urllib
import logging
import time
import sys
import boto3
from botocore.exceptions import ClientError


def _get_client():
    client = boto3.client('s3')
    return client


def _list_bucket_objects(s3_client, bucket_name):
    """List the objects in an Amazon S3 bucket

    :param bucket_name: string
    :return: List of bucket objects. If error, return None.
    """

    # Retrieve the list of bucket objects
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
    except ClientError as e:
        # TODO(milos) we should have a different response if bucket is not found vs if something else went wrong
        # AllAccessDisabled error == bucket not found
        logging.error(e)
        return None

    # Only return the contents if we found some keys
    if response['KeyCount'] > 0:
        return response['Contents']

    return None


def _clear_bucket(s3_client, bucket_name):
    """Clear the objects in an Amazon S3 bucket

    :param bucket_name: string
    :return: True if successful. If error, return None.
    """

    objects = _list_bucket_objects(s3_client, bucket_name)
    if objects is not None:
        file_names = []
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            file_names.append(file_key)
        if len(file_names) >= 1:
            _delete_objects(s3_client, bucket_name, file_names)
    return True


def _delete_object(s3_client, bucket_name, object_name):
    """Delete an object from an S3 bucket

    :param bucket_name: string
    :param object_name: string
    :return: True if the referenced object was deleted, otherwise False
    """
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# Delete multiple objects from an Amazon S3 bucket
def _delete_objects(s3_client, bucket_name, object_names):
    """Delete multiple objects from an Amazon S3 bucket

    :param bucket_name: string
    :param object_names: list of strings
    :return: True if the referenced objects were deleted, otherwise False
    """
    # Convert list of object names to appropriate data format
    obj_list = [{'Key': obj} for obj in object_names]

    # Delete the objects
    try:
        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': obj_list})
    except ClientError as e:
        logging.error(e)
        return False
    return True


# Fetch an file from an Amazon S3 bucket to local path
def _download_file(s3_client, bucket_name, object_name, local_path):
    """Fetch an file to an Amazon S3 bucket to local path

    The src_data argument must be of type bytes or a string that references a file specification.

    :param dest_bucket_name: string
    :param dest_file_key: string
    :return: True if get the file successfully, otherwise False
    """

    try:
        s3_client.download_file(bucket_name, object_name, local_path)
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(e)
        return False

    return True


def _upload_file(s3_client, bucket_name, object_name, local_path):
    """Add a local file to an Amazon S3 bucket

    The src_data argument must be of type bytes or a string that references a file specification.

    :param bucket_name: string
    :param object_name: string
    :param local_path: string
    :return: True if file was added to dest_bucket/dest_file_key, otherwise False
    """

    # upload_file
    try:
        s3_client.upload_file(local_path, bucket_name, object_name)
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(e)
        return False
    return True


def _get_object(s3_client, bucket_name, object_name):
    """Retrieve an object from an Amazon S3 bucket

    :param bucket_name: string
    :param object_name: string
    :return: botocore.response.StreamingBody object. If error, return None.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        # AllAccessDisabled error == bucket or object not found
        # TODO(milos) I think the exception does not get caught if the object does  not exist
        logging.error(e)
        return None
    # Return an open StreamingBody object
    # TODO(milos) why do we return this weird iterator, is there a better way to do this?
    return response['Body']


# Retrieve an object from an Amazon S3 bucket, or wait if not exist,
def _get_object_or_wait(s3_client, bucket_name, object_name, sleep_time, time_out=60):
    """Retrieve an object from an Amazon S3 bucket, or wait if not exist,

    :param bucket_name: string
    :param object_name: string
    :param sleep_time: float
    :param time_out: int (seconds)
    :return: botocore.response.StreamingBody object. If error, return None.
    """
    start_time = time.time()
    while True:
        # TODO(milos) check how is this timeout value set
        if time.time() - start_time > time_out:
            return None
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
            # Return an open StreamingBody object
            return response['Body']
        except ClientError as e:
            # AllAccessDisabled error == bucket or object not found
            time.sleep(sleep_time)


def _put_object(s3_client, bucket_name, object_name, src_data):
    """Add an object to an Amazon S3 bucket

    The src_data argument must be of type bytes or a string that references a file specification.

    :param bucket_name: string
    :param object_name: string
    :param src_data: bytes of data or string reference to file spec
    :return: True if src_data was added to dest_bucket/dest_object, otherwise False
    """
    # Construct Body= parameter
    if isinstance(src_data, bytes):
        object_data = src_data
    elif isinstance(src_data, str):
        try:
            object_data = open(src_data, 'rb')
            # possible FileNotFoundError/IOError exception
        except Exception as e:
            logging.error(e)
            return False
    else:
        logging.error('Type of ' + str(type(src_data)) +
                      ' for the argument \'src_data\' is not supported.')
        return False

    # Put the object
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=object_data)
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(e)
        return False
    finally:
        if isinstance(src_data, str):
            object_data.close()
    return True

def _create_bucket(s3_client, bucket_name):
    # NOTE no reason to have the location specified to be eu-central-1
    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})

def _delete_bucket(s3_client, bucket_name):
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

    
