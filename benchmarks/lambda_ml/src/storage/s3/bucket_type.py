from src.storage.s3 import bucket_operator
from src.storage.base import BaseStorage
from typing import TypeVar, Union, Iterator, List
from boto3 import Session
import uuid

Boto3Session = TypeVar('Boto3Session', Session, None)

class S3Bucket(BaseStorage):

    def __init__(self, client: Boto3Session = None, name: str = "", timeout: int = 60,
                 create_new: bool = False) -> None:
        self.client = bucket_operator._get_client() if client is None else client
        self.name = "lambdaml-" + str(uuid.uuid4()) if name == "" else name
        self.timeout = timeout
        if create_new or name == "":
            self.__create_new_bucket()

    def save(self, src_data: Union[str, bytes], key: str) -> bool:
        return bucket_operator._put_object(self.client, self.name, key, src_data)

    def load(self, key: str) -> Iterator[bytes]:
        return bucket_operator._get_object(self.client, self.name, key)

    def load_or_wait(self, key: str, sleep_time: float = 0.1) -> Iterator[bytes]:
        return bucket_operator._get_object_or_wait(self.client, self.name, key, sleep_time, self.timeout)

    def delete(self, key: Union[str, List[str]]) -> bool:
        if isinstance(key, str):
            return bucket_operator._delete_object(self.client, self.name, key)
        return bucket_operator._delete_objects(self.client, self.name, key)

    def clear(self, delete_bucket: bool = False) -> bool:
        success = bucket_operator._clear_bucket(self.client, self.name)
        if success and delete_bucket:
            return bucket_operator._delete_bucket(self.client, self.name)
        return success

    def list(self) -> List[Iterator[bytes]]:
        return bucket_operator._list_bucket_objects(self.client, self.name)

    def __create_new_bucket(self):
        bucket_operator._create_bucket(self.client, self.name)
