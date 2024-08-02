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


def counter(client,key):
    """increment by 1 under specific key
    
    :param client: redis.client
    :param key: string
    :return: True if it's connected 
    """
    
    # Connect to redis
    #r = redis.Redis(host=endpoint, port=6379, db=0)
    # Set the object
    try:
        client.incrby(key, 1)
    except ClientError as e:
        # AllAccessDisabled error == endpoint not found
        logging.error(e)
        return False
    
    return True


def hcounter(client, key, field):
    """increment by 1 under specific field of the key
    
    :param client: redis.client
    :param key: string
    :param field: string
    :return: True if it's connected 
    """
    
    # Connect to redis
    #r = redis.Redis(host=endpoint, port=6379, db=0)
    # Set the object
    try:
        client.hincrby(key, field, 1)
    except ClientError as e:
        # AllAccessDisabled error == endpoint not found
        logging.error(e)
        return False
    
    return True
        

def handler(event, context):
    endpoint = "test.fifamc.ng.0001.euc1.cache.amazonaws.com"
    client = redis.Redis(host=endpoint, port=6379, db=0)
    print(counter(client,"LambdaML"))

             
            
    
            
        


