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
import memcache
from botocore.exceptions import ClientError


def memcached_init(endpoint):
    port = 11211
    try:
        endpoint = endpoint+":"+str(port)
        client = memcache.Client([endpoint])
    except ClientError as e:
        # AllAccessDisabled error == endpoint not found
        logging.error(e)
        return False
    return client
