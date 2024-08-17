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
from botocore.exceptions import ClientError
from archived.elasticache import get_object


def set_object(client, key, src_data):
    """Add value from configured redis under specified key
    
    :param endpoint: string
    :param key: string
    :param src_data: bytestream, int, string
    :return: True if src_data was added to redis at endpoint under key, otherwise False
    """
    if isinstance(src_data,bytes) or isinstance(src_data,str) or isinstance(src_data,int):
        object_data = src_data
    else:
        logging.error('Type of ' + str(type(src_data)) +
                      ' for the argument \'src_data\' is not supported.')
    # Connect to redis
    #r = redis.Redis(host=endpoint, port=6379, db=0)
    # Set the object
    try:
        response = client.set(name = key,value = object_data)
    except ClientError as e:
        # AllAccessDisabled error == client lost
        logging.error(e)
        return False
    return True
        

def hset_object(client, key, field, src_data):
    """Add value from configured redis under specified key of certain hashtable
    
    :param endpoint: string
    :param field: string
    :param key: string
    :param src_data: bytesstream, int, string
    :return: True if src_data was added to redis at endpoint under key, otherwise False
    """
    if isinstance(src_data,bytes) or isinstance(src_data,str) or isinstance(src_data,int):
        object_data = src_data
    else:
        logging.error('Type of ' + str(type(src_data)) +
                      ' for the argument \'src_data\' is not supported.')
    # Connect to redis
    #client = redis.Redis(host=endpoint, port=6379, db=0)
    # Set the object
    try:
        response = client.hset(name = key,key = field,value = object_data)
    except ClientError as e:
        # AllAccessDisabled error == client lost
        logging.error(e)
        return False
    return True
        

def handler(event, context):
    endpoint = "test-001.fifamc.0001.euc1.cache.amazonaws.com"
    key = "lambdaml"
    value = 1
    set_object(endpoint,key,value)
    print(get_object(endpoint,key))
             
            
    
            
        




             
            
    
            
        


