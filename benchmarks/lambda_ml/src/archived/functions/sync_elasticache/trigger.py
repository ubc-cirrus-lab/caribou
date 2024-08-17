#making sure to empty the bucket before start.


def main(event, context):

    storage = event['storage']
    if storage == "memcache":
        from archived.elasticache.Memcached import memcached_init
        from archived.elasticache.Memcached.clear_all import clear_all

        memcache_location = event['elasticache']
        endpoint = memcached_init(redis_location)

        clear_all(endpoint)
    if storage == "redis":
        from archived.elasticache import redis_init
        from archived.sync import clear_bucket

        redis_location = event['elasticache']
        grad_bucket = event['grad_bucket']
        model_bucket = event['model_bucket']
        endpoint = redis_init(redis_location)

        clear_bucket(endpoint,grad_bucket)
        clear_bucket(endpoint,model_bucket)
