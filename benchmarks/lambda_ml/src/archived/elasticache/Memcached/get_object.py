import logging
import memcache
from botocore.exceptions import ClientError
import time


def hget_object(client, key, field):
    """Retrieve an object from configured memcache under specified key
    :param client: memcache client object
    :param key: string
    :param field: string
    :return: direct bytestream, no need to read from buffer. If error, return None.
    """
    try:
        mem_key = key + "_" + field
        response = client.get(key=mem_key)
    except ClientError as e:
        # AllAccessDisabled error == client lost
        logging.error(e)
        return None
    return response


def hget_object_or_wait(client, key, field, sleep_time):
    """Retrieve an object from configured memcache under specified key

    :param client: memcache client object
    :param key: string
    :param field: string
    :param sleep_time: float
    :return: direct bytestream, no need to read from buffer. If error, return None.
    """
    try:
        mem_key = key + "_" + field
        while True:
            response = client.get(key=mem_key)
            if response != None:
                return response
            time.sleep(sleep_time)
    except ClientError as e:
        # AllAccessDisabled error == client lost
        logging.error(e)
        return None
