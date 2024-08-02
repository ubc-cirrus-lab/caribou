from src.storage.base import BaseStorage
from src.storage.s3.s3_type import S3Storage
from src.storage.s3.bucket_type import S3Bucket
from src.storage.memcached.memcached_type import MemcachedStorage
from src.storage.redis.redis_type import RedisStorage
from src.storage.dynamo.dynamo_type import DynamoTable
import src.storage.dynamo.dynamo_operator as dynamo_operator
