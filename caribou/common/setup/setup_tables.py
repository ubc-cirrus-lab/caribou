import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

from caribou.common import constants

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Only add a StreamHandler if not running in AWS Lambda
if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())


def create_table(dynamodb, table_name) -> bool:
    # Check if the table already exists
    try:
        dynamodb.describe_table(TableName=table_name)
        logger.info("Table %s already exists", table_name)
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    # Create all non sync tables with on-demand billing mode
    dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "key", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )

    return True


def create_bucket(s3, bucket_name):
    # Check if the bucket already exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info("Bucket %s already exists", bucket_name)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "404" and e.response["Error"]["Code"] != "403":
            raise
    s3.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": constants.GLOBAL_SYSTEM_REGION}
    )


def main():
    dynamodb = boto3.client("dynamodb", region_name=constants.GLOBAL_SYSTEM_REGION)

    # Disabled as part of issue #293
    # s3 = boto3.client("s3", region_name=constants.GLOBAL_SYSTEM_REGION)

    # Get all attributes of the constants module
    for attr in dir(constants):
        # If the attribute name ends with '_TABLE', create a DynamoDB table
        if attr.endswith("_TABLE"):
            table_name = getattr(constants, attr)
            if table_name in [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]:
                continue

            created_table: bool = False
            try:
                created_table = create_table(dynamodb, table_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Error creating table %s: %s", table_name, e)
                logger.error("Trying to create table again")
                try:
                    time.sleep(1)  # Sleep for 1 second before trying to create the table again
                    created_table = create_table(dynamodb, table_name)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("Error creating table %s: %s", table_name, e)
                    logger.error("Skipping table creation")

            if created_table:
                logger.info("Created table: %s", table_name)

        # Disabled as part of issue #293
        # # If the attribute name ends with '_BUCKET', create an S3 bucket
        # elif attr.endswith("_BUCKET"):
        #     # Allow for the bucket name to be overridden by an environment variable
        #     bucket_name = os.environ.get(f"CARIBOU_OVERRIDE_{attr}", getattr(constants, attr))
        #     print("bucket_name", bucket_name)
        #     print(f"attr: {attr}")
        #     logger.info("Creating bucket: %s", bucket_name)
        #     create_bucket(s3, bucket_name)


if __name__ == "__main__":
    main()
