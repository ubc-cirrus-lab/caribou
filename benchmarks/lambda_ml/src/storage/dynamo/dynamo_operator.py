import time
import logging

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import Binary
from botocore.exceptions import ClientError


def get_client():
    dynamodb = boto3.resource('dynamodb')
    return dynamodb


def get_table(client, tb_name):
    return client.Table(tb_name)


def put_item(table, key_col, key, src_data):
    """Add an object to a dynamodb table
    The src_data argument must be of type bytes.

    :param table: dynamodb table
    :param key_col: string
    :param key: string
    :param src_data: bytes of data
    :return: True if src_data was added to table, otherwise False
    """
    assert isinstance(src_data, bytes)
    try:
        table.put_item(
            Item={
                '{}'.format(key_col): key,
                'value': src_data
            }
        )
    except ClientError as e:
        logging.error("put_item: {}".format(e.response['Error']['Message']))
        return False
    return True


def get_item(table, key_col, key):
    """Retrieve an item from a dynamodb table
    The src_data argument must be of type bytes or a string that references a file specification.

    :param table: dynamodb table
    :param key_col: string
    :param key: string
    :return: dynamodb item, None if encounters error
    """
    try:
        response = table.get_item(Key={'{}'.format(key_col): key})
    except ClientError as e:
        logging.error("get_item: {}".format(e.response['Error']['Message']))
        return None
    return response['Item']


def get_item_or_wait(table, key_col, key, sleep_time, time_out=60):
    """Retrieve an item from an Amazon S3 bucket, or wait if not exist,

    :param table: dynamodb table
    :param key_col: string
    :param key: string
    :param sleep_time: float
    :param time_out: int (seconds)
    :return: dynamodb item, None if timeout
    """
    start_time = time.time()
    while True:
        if time.time() - start_time > time_out:
            return None
        try:
            response = table.query(
                KeyConditionExpression=Key(key_col).eq(key)
            )
            if len(response['Items']) == 1 and response['Items'][0] is not None:
                return response['Items'][0]
        except ClientError as e:
            logging.error("get_item_or_wait: {}".format(e.response['Error']['Message']))
        time.sleep(sleep_time)


def delete_item(table, key_col, key):
    """Delete an item from a dynamodb table

    :param table: dynamodb table
    :param key_col: string
    :param key: string
    :return: True if the referenced item was deleted, otherwise False
    """
    try:
        response = table.delete_item(Key={
            key_col: key
        })
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_items(table, key_col, keys):
    """Delete an item from a dynamodb table

    :param table: dynamodb table
    :param key_col: string
    :param keys: list of string
    :return: True if the referenced item was deleted, otherwise False
    """
    try:
        for key in keys:
            response = table.delete_item(Key={
                key_col: key
            })
    except ClientError as e:
        logging.error(e)
        return False
    return True


def list_items(table):
    """List all the items in a dynamodb table

    :param table: dynamodb table
    :return: List of items. If error, return None.
    """
    try:
        response = table.scan()
        return response['Items']
    except ClientError as e:
        logging.error(e)
        return None


def clear_table(table, key_col):
    """Clear all the items in a dynamodb table

    :param table: dynamodb table
    :param key_col: string
    :return: True if successful. If error, return None.
    """

    items = list_items(table)
    if items is not None and len(items) >= 1:
        keys = []
        try:
            for item in items:
                key = item[key_col]
                keys.append(key)
            delete_items(table, key_col, keys)
        except ClientError as e:
            logging.error(e)
            return False
    return True
