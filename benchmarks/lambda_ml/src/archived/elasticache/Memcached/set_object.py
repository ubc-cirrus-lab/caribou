import logging
from botocore.exceptions import ClientError
from archived.elasticache.Memcached import hlist_keys


def hset_object(client, key, field, src_data):
    """Add value from configured memcache under specified key of certain hashtable

    :param client: string
    :param field: string
    :param key: string
    :param src_data: bytesstream, int, string
    :return: True if src_data was added to memcache at client under key, otherwise False
    """
    if isinstance(src_data, bytes) or isinstance(src_data, str) or isinstance(src_data, int):
        object_data = src_data
    else:
        logging.error('Type of ' + str(type(src_data)) +
                      ' for the argument \'src_data\' is not supported.')
    #mc = client
    #mc = memcache.Client([client+":"+str(11211)],debug=True)
    # Connect to memcache
    #client = memcache.memcache(host=client, port=6379, db=0)
    # Set the object
    try:
        mem_key = key + "_" + field
        response = client.set(mem_key, object_data)
    except ClientError as e:
        logging.error(e)
        return False
    return response


def handler(event,context):
    mc = 'convergence.fifamc.cfg.euc1.cache.amazonaws.com'
    #mc = memcache.Client(['convergence.fifamc.cfg.euc1.cache.amazonaws.com:11211'])
    print(hset_object(mc, "tmp_value","g_1", bytes("haha", 'utf-8')))
    print(hlist_keys(mc, ["tmp_value_g_1", "tmp_value_g_2"]))
