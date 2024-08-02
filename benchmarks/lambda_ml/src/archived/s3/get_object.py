# Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# This file is licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of the
# License is located at
#
# http://aws.amazon.com/apache2.0/
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import logging
import time
import boto3
from botocore.exceptions import ClientError


def get_object(bucket_name, object_name):
    """Retrieve an object from an Amazon S3 bucket

    :param bucket_name: string
    :param object_name: string
    :return: botocore.response.StreamingBody object. If error, return None.
    """

    # Retrieve the object
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        # AllAccessDisabled error == bucket or object not found
        logging.error(e)
        return None
    # Return an open StreamingBody object
    return response['Body']


def get_object2(s3_client, bucket_name, object_name):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        # AllAccessDisabled error == bucket or object not found
        logging.error(e)
        return None
    # Return an open StreamingBody object
    return response['Body']


def get_object_or_wait(bucket_name, object_name, sleep_time):
    """Retrieve an object from an Amazon S3 bucket

    :param bucket_name: string
    :param object_name: string
    :param sleep_time: float
    :return: botocore.response.StreamingBody object. If error, return None.
    """

    # Retrieve the object
    s3 = boto3.client('s3')

    while True:
        try:
            response = s3.get_object(Bucket=bucket_name, Key=object_name)
            # Return an open StreamingBody object
            return response['Body']
        except ClientError as e:
            # AllAccessDisabled error == bucket or object not found
            time.sleep(sleep_time)


def get_object_or_wait2(s3_client, bucket_name, object_name, sleep_time):
    while True:
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
            # Return an open StreamingBody object
            return response['Body']
        except ClientError as e:
            # AllAccessDisabled error == bucket or object not found
            time.sleep(sleep_time)


def main():
    """Exercise get_object()"""

    # Assign these values before running the program
    test_bucket_name = 'BUCKET_NAME'
    test_object_name = 'OBJECT_NAME'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # Retrieve the object
    stream = get_object(test_bucket_name, test_object_name)
    if stream is not None:
        # Read first chunk of the object's contents into memory as bytes
        data = stream.read(amt=1024)
        # Output object's beginning characters
        logging.info(f'{test_object_name}: {data[:60]}...')


if __name__ == '__main__':
    main()
