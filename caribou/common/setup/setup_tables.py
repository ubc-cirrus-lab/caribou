import logging
import os

import boto3

from caribou.common import constants
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient

logger = logging.getLogger()


def create_table(dynamodb, table_name):
    # Check if the table already exists
    try:
        dynamodb.describe_table(TableName=table_name)
        logger.info("Table %s already exists", table_name)
        return
    except dynamodb.exceptions.ResourceNotFoundException:
        pass
    if table_name in [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]:
        client = AWSRemoteClient(constants.GLOBAL_SYSTEM_REGION)
        client.create_sync_tables()
    dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )


def create_bucket(s3, bucket_name):
    # Check if the bucket already exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info("Bucket %s already exists", bucket_name)
        return
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404" and e.response["Error"]["Code"] != "403":
            raise
    s3.create_bucket(Bucket=bucket_name)


def main():
    dynamodb = boto3.client("dynamodb", region_name=constants.GLOBAL_SYSTEM_REGION)
    s3 = boto3.client("s3", region_name="us-east-1")

    # Get all attributes of the constants module
    for attr in dir(constants):
        # If the attribute name ends with '_TABLE', create a DynamoDB table
        if attr.endswith("_TABLE"):
            table_name = getattr(constants, attr)
            logger.info("Creating table: %s", table_name)
            create_table(dynamodb, table_name)
        # If the attribute name ends with '_BUCKET', create an S3 bucket
        elif attr.endswith("_BUCKET"):
            # Allow for the bucket name to be overridden by an environment variable
            bucket_name = os.environ.get(f"CARIBOU_OVERRIDE_{attr}", getattr(constants, attr))
            logger.info("Creating bucket: %s", bucket_name)
            create_bucket(s3, bucket_name)


if __name__ == "__main__":
    main()
