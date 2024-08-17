import logging
import memcache
from botocore.exceptions import ClientError


def hdelete_keys(client, fields):
    try:
        client.delete_multi(fields)
    except ClientError as e:
        # AllAccessDisabled error ==  client lost
        logging.error(e)
        return False

    return True
