import boto3

from multi_x_serverless.common import constants


def create_table(dynamodb, table_name):
    # Check if the table already exists
    try:
        dynamodb.describe_table(TableName=table_name)
        print(f"Table {table_name} already exists")
        return
    except dynamodb.exceptions.ResourceNotFoundException:
        pass
    dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )


def create_bucket(s3, bucket_name):
    s3.create_bucket(Bucket=bucket_name)


def main():
    dynamodb = boto3.client("dynamodb")
    s3 = boto3.client("s3", region_name=constants.GLOBAL_SYSTEM_REGION)

    # Get all attributes of the constants module
    for attr in dir(constants):
        # If the attribute name ends with '_TABLE', create a DynamoDB table
        if attr.endswith("_TABLE"):
            table_name = getattr(constants, attr)
            print(f"Creating table: {table_name}")
            create_table(dynamodb, table_name)
        # If the attribute name ends with '_BUCKET', create an S3 bucket
        elif attr.endswith("_BUCKET"):
            bucket_name = getattr(constants, attr)
            print(f"Creating bucket: {bucket_name}")
            create_bucket(s3, bucket_name)


if __name__ == "__main__":
    main()
