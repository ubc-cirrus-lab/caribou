import decimal
from functools import reduce

import boto3
import networkx as nx
from boto3.dynamodb.conditions import Key

AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
GRID_CO2_TABLE_NAME = "multi-x-serverless-datacenter-grid-co2"
TRANSMISSION_CO2_TABLE_NAME = "multi-x-serverless-transmission-co2"
LATENCY_TABLE_NAME = "multi-x-serverless-network-latencies"
# From https://www.cloudcarbonfootprint.org/docs/methodology/
ENERGY_CONSUMPTION_PER_GB = 0.001
AWS = "aws"


def convert_decimals_to_float(item: dict) -> dict:
    """
    Converts all decimals in a dictionary to floats.
    """
    for key, value in item.items():
        if isinstance(value, decimal.Decimal):
            item[key] = float(value)
    return item


def get_item_from_dynamodb(key: dict, table_name: str, limit: int = -1, order: str = "asc") -> list[dict]:
    """
    Gets an item from a DynamoDB table

    key: dict with the key of the item to get
    table_name: name of the table
    """
    # table_name = table_name + '_'

    dynamodb = boto3.resource(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )

    # First check if table exists
    try:
        # Table exists
        table = dynamodb.Table(table_name)
        table.load()
    except dynamodb.meta.client.exceptions.ResourceNotFoundException as e:
        return []  # Table does not exist or cannot be accessed

    # If table exists, we can try accessing elements
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

    if "Items" in response:
        items = response["Items"]
        items = [convert_decimals_to_float(item) for item in items]
    else:
        items = response["Item"]
        items = [convert_decimals_to_float(items)]

    return items


def get_dag(workflow_description: dict) -> nx.DiGraph:
    dag = nx.DiGraph()
    for function in workflow_description["functions"]:
        dag.add_node(function["name"])
        for next_function in function["next_functions"]:
            dag.add_edge(function["name"], next_function["name"])
    return dag
