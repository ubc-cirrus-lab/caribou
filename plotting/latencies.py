import boto3

from functools import reduce
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from boto3.dynamodb.conditions import Key

DEFAULT_REGION = "us-west-2"

LATENCY_TABLE_NAME = "multi-x-serverless-network-latencies"
DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"

DYNAMO_DB = boto3.resource(
    "dynamodb",
    region_name=DEFAULT_REGION,
)

def get_item_from_dynamodb(key: dict, table_name: str, limit: int = -1, order: str = "asc") -> dict:
    """
    Gets an item from a DynamoDB table

    key: dict with the key of the item to get
    table_name: name of the table
    """
    
    table = DYNAMO_DB.Table(table_name)
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



table = DYNAMO_DB.Table(DATACENTER_INFO_TABLE_NAME)

response = table.scan()

regions = []
for item in response["Items"]:
    regions.append((item["region_code"], item["provider"]))

from_to_codes = []
avg_latencies = []
_50th_latencies = []
_90th_latencies = []
_95th_latencies = []
_99th_latencies = []

for region in regions:
    for region2 in regions:
        region_from_to_code = region[1] + ":" + region[0] + ":" + region2[1] + ":" + region2[0]
        latency_coefficient = get_item_from_dynamodb(
            {
                "region_from_to_codes": region_from_to_code,
            },
            LATENCY_TABLE_NAME,
            limit=1,
            order="desc",
        )

        if len(latency_coefficient) == 0:
            continue
        from_to_codes.append(region_from_to_code)
        avg_latencies.append(latency_coefficient[0]["average"])
        _50th_latencies.append(latency_coefficient[0]["50th"])
        _90th_latencies.append(latency_coefficient[0]["90th"])
        _95th_latencies.append(latency_coefficient[0]["95th"])
        _99th_latencies.append(latency_coefficient[0]["99th"])

data = {
    "From": [code.split(":")[1] for code in from_to_codes],
    "To": [code.split(":")[3] for code in from_to_codes],
    "Avg Latency": avg_latencies,
    "50th Latency": _50th_latencies,
    "90th Latency": _90th_latencies,
    "95th Latency": _95th_latencies,
    "99th Latency": _99th_latencies,
}

df = pd.DataFrame(data)
metrics = ["Avg Latency", "50th Latency", "90th Latency", "95th Latency", "99th Latency"]

latency_columns = ["Avg Latency", "50th Latency", "90th Latency", "95th Latency", "99th Latency"]
df[latency_columns] = df[latency_columns].apply(pd.to_numeric, errors="coerce")

# Create a pivot table
pivot_df = df.pivot_table(index="From", columns="To")

order = [
    'ap-southeast-1', 'ap-southeast-2',  # Asia Pacific (Singapore, Sydney)
    'ap-northeast-1', 'ap-northeast-2', 'ap-northeast-3',  # Asia Pacific (Tokyo, Seoul, Osaka)
    'ap-south-1',  # Asia Pacific (Mumbai)
    'eu-central-1',  # EU (Frankfurt)
    'eu-north-1',  # EU (Stockholm)
    'eu-west-1', 'eu-west-2', 'eu-west-3',  # EU (Ireland, London, Paris)
    'ca-central-1',  # Canada (Central)
    'us-east-1', 'us-east-2',  # US East (N. Virginia, Ohio)
    'us-west-1', 'us-west-2'  # US West (N. California, Oregon)
    'sa-east-1',  # South America (Sao Paulo)
]

for metric in metrics:
    plt.figure(figsize=(12, 8))
    local_df = pivot_df[metric].copy()
    local_df = local_df.reindex(index=order, columns=order)
    sns.heatmap(local_df, annot=True, fmt=".2f", cmap="YlGnBu", annot_kws={"size": 4})
    plt.title(f"Latency for transmitting 1MB of data, {metric}")
    plt.tight_layout()

    plt.savefig(f"plots/latencies_{metric}.png", dpi=300)
