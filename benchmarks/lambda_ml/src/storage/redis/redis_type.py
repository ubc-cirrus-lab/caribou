from src.storage.base import BaseStorage
from src.storage.redis import redis_operator


class RedisStorage(BaseStorage):

    def __init__(self, name, ip, port):
        super(RedisStorage, self).__init__(name)
        self.client = redis_operator.get_client(ip, port)

    def save(self, src_data, keys, **kwargs):
        return redis_operator.set_object(self.client, keys, src_data)

    def save_v2(self, src_data, keys, bucket_name, **kwargs):
        return redis_operator.hset_object(self.client, bucket_name, keys, src_data)

    def load(self, key, **kwargs):
        return redis_operator.get_object(self.client, key)

    def load_or_wait(self, key, sleep_time=0.1, time_out=60):
        return redis_operator.get_object_or_wait(self.client, key, sleep_time, time_out)

    def load_v2(self, key, bucket_name, **kwargs):
        return redis_operator.hget_object(self.client, bucket_name, key)

    def load_or_wait_v2(self, key, bucket_name, sleep_time=0.1, time_out=60):
        return redis_operator.hget_object_or_wait(self.client, bucket_name, key, sleep_time, time_out)

    def delete(self, key, **kwargs):
        return redis_operator.delete_keys(self.client, keys)

    def delete_v2(self, keys, bucket_name):
        return redis_operator.hdelete_keys(self.client, bucket_name, keys)

    def clear(self, **kwargs):
        return redis_operator.clear_all(self.client)

    def list(self):
        return redis_operator.list_keys(self.client, count=10000)

    def list_v2(self, bucket_name):
        return redis_operator.hlist_keys(self.client, bucket_name, count=10000)
