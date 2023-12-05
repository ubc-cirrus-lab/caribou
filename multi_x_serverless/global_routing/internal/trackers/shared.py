import datetime
import os

import boto3

# from google.auth import credentials as ga_credentials

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")


def get_curr_region() -> str:
    my_session = boto3.session.Session()
    my_region = my_session.region_name
    if my_region:
        return my_region
    return "us-west-2"


def write_results(results: dict, table_name: str) -> None:
    client = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    client.batch_write_item(RequestItems={table_name: results})


def get_current_time() -> str:
    time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return time


def get_aws_regions() -> list[dict]:
    region = get_curr_region()
    client = boto3.client("ec2", region)
    response = client.describe_regions()
    return response["Regions"]


def create_aws_table_if_not_exists(
    table_name: str, key_schema: list, attribute_definitions: list, provisioned_throughput: dict
) -> None:
    """
    Creates a DynamoDB table if it does not exist yet

    table_name: name of the table
    key_schema: list of dicts with the key schema
    attribute_definitions: list of dicts with the attribute definitions
    provisioned_throughput: dict with the provisioned throughput
    """
    dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-west-2",
    )

    existing_tables = [table.name for table in dynamodb.tables.all()]

    if table_name not in existing_tables:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=provisioned_throughput,
        )
        table.wait_until_exists()


def get_item_from_dynamodb(key: dict, table_name: str) -> dict:
    """
    Gets an item from a DynamoDB table

    key: dict with the key of the item to get
    table_name: name of the table
    """
    dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-west-2",
    )
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key)
    return response["Item"]
