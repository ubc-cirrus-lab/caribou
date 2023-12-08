from functools import reduce

import boto3
import networkx as nx
from boto3.dynamodb.conditions import Key

AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
DEFAULT_REGION = "us-west-2"
GRID_CO2_TABLE_NAME = "multi-x-serverless-datacenter-grid-co2"
TRANSMISSION_CO2_TABLE_NAME = "multi-x-serverless-transmission-co2"
LATENCY_TABLE_NAME = "multi-x-serverless-network-latencies"
# From https://www.cloudcarbonfootprint.org/docs/methodology/
ENERGY_CONSUMPTION_PER_GB = 0.001
AWS = "aws"
OPT_IN_REGIONS = [
    'af-south-1',
    'ap-east-1',
    'ap-south-2',
    'ap-southeast-3',
    'ap-southeast-4',
    'eu-south-1',
    'eu-south-2',
    'eu-central-2',
    'me-south-1',
    'me-central-1',
    'il-central-1'
]

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

    if "Items" in response:
        return response["Items"]

    return response["Item"]


def get_dag(workflow_description: dict) -> nx.DiGraph:
    dag = nx.DiGraph()
    for function in workflow_description["functions"]:
        dag.add_node(function["name"])
        for next_function in function["next_functions"]:
            dag.add_edge(function["name"], next_function["name"])
    return dag
