import logging

import boto3

from multi_x_serverless.common import constants

logger = logging.getLogger()


def create_table(dynamodb, table_name):
    # Check if the table already exists
    try:
        dynamodb.describe_table(TableName=table_name)
        logger.info("Table %s already exists", table_name)
        return
    except dynamodb.exceptions.ResourceNotFoundException:
        pass
    if table_name == constants.WORKFLOW_SUMMARY_TABLE:
        # Create the table with a sort key
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "key", "AttributeType": "S"},
                {"AttributeName": "sort_key", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "key", "KeyType": "HASH"},
                {"AttributeName": "sort_key", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        return
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
    dynamodb = boto3.client("dynamodb")
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
            bucket_name = getattr(constants, attr)
            logger.info("Creating bucket: %s", bucket_name)
            create_bucket(s3, bucket_name)


if __name__ == "__main__":
    main()
