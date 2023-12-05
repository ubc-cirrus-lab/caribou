from functools import reduce

import boto3
from boto3.dynamodb.conditions import Key

AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
DEFAULT_REGION = "us-west-2"
GRID_CO2_TABLE_NAME = "multi-x-serverless-datacenter-grid-co2"


def get_item_from_dynamodb(key: dict, table_name: str, limit: int = -1, order: str = "asc") -> dict:
    """
    Gets an item from a DynamoDB table

    key: dict with the key of the item to get
    table_name: name of the table
    """
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )
    table = dynamodb.Table(table_name)
    if limit < 1:
        response = table.get_item(Key=key)
    else:
        key_conditions = [Key(k).eq(key[k]) for k in key]
        if order == "asc":
            response = table.query(KeyConditionExpression=reduce(lambda x, y: x & y, key_conditions), Limit=limit)
        elif order == "desc":
            response = table.query(
                KeyConditionExpression=reduce(lambda x, y: x & y, key_conditions), Limit=limit, ScanIndexForward=False
            )
    return response["Items"]
