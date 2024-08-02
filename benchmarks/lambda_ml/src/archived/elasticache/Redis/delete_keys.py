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
import redis
from botocore.exceptions import ClientError


def delete_keys(client, keys):
    """Delete keys in configured redis
    
    :param client: redis client object
    :param keys: string or list of strings
    :return: True if the reference objects were deleted or don't exsit, otherwise False 
    """
    
    # Connect to redis
    #r = redis.Redis(host=endpoint, port=6379, db=0)

    try:
        nums = client.delete(*keys) 
    except ClientError as e:
        # AllAccessDisabled error == client lost
        logging.error(e)
        return False
    return True


def hdelete_keys(client, key, fields):
    """delete the field within hash key in configured redis
    
    :param client: redis client object
    :param key: string
    :param fields: string or list of strings
    :return: True if the reference objects were deleted or don't exsit, otherwise False 
    """
    
    # Connect to redis
    #r = redis.Redis(host=endpoint, port=6379, db=0)

    
    try:
        client.hdel(key, *fields)
    except ClientError as e:
        # AllAccessDisabled error ==  client lost
        logging.error(e)
        return False
    
    return True
        

def handler(event, context):
    from archived.elasticache import hlist_keys
    location = "test.fifamc.ng.0001.euc1.cache.amazonaws.com"
    endpoint = redis.Redis(host = location, port = 6379, db = 0)
    heyhey = hlist_keys(endpoint,"tmp-updates")
    print(heyhey)
    hdelete_keys(endpoint,"tmp-updates",he)
    print(hlist_keys(endpoint,"tmp-updates"))