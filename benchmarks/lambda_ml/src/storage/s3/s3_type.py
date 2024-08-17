from src.storage.s3 import s3_operator
from src.storage.base import BaseStorage


class S3Storage(BaseStorage):

    def __init__(self):
        super(S3Storage, self).__init__()
        self.client = s3_operator.get_client()

    def save(self, src_data, object_name, bucket_name=""):
        return s3_operator.put_object(self.client, bucket_name, object_name, src_data)

    def load(self, object_name, bucket_name=""):
        return s3_operator.get_object(self.client, bucket_name, object_name)

    def load_or_wait(self, object_name, bucket_name="", sleep_time=0.1, time_out=60):
        return s3_operator.get_object_or_wait(self.client, bucket_name, object_name, sleep_time, time_out)

    def delete(self, key, bucket_name=""):
        if isinstance(key, str):
            return s3_operator.delete_object(self.client, bucket_name, key)
        elif isinstance(key, list):
            return s3_operator.delete_objects(self.client, bucket_name, key)

    def clear(self, bucket_name=""):
        return s3_operator.clear_bucket(self.client, bucket_name)

    def list(self, bucket_name=""):
        return s3_operator.list_bucket_objects(self.client, bucket_name)

    def download(self, bucket_name, object_name, local_path):
        return s3_operator.download(self.client, bucket_name, object_name, local_path)

    def upload(self, bucket_name, object_name, local_path):
        return s3_operator.upload(self.client, bucket_name, object_name, local_path)
