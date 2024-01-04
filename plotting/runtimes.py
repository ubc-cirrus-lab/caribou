import boto3

from functools import reduce
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

from boto3.dynamodb.conditions import Key

DEFAULT_REGION = "us-west-2"

RUNTIME_TABLE_NAME = "multi-x-serverless-runtime-tracker"
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

# print(regions)





def retrieve_runtime_information(coumpound_key):
    run_time_information = get_item_from_dynamodb(
        {
            "compound": coumpound_key,
        },
        RUNTIME_TABLE_NAME,
        limit=1,
        order="desc",
    )

    if len(run_time_information) == 0:
        return None

    return {
        "average": float(run_time_information[0]["execution_mean"]),
        "50th": float(run_time_information[0]["execution_50th_percentile"]),
        "90th": float(run_time_information[0]["execution_90th_percentile"]),
        "95th": float(run_time_information[0]["execution_95th_percentile"]),
        "99th": float(run_time_information[0]["execution_99th_percentile"]),
    }
    

image_function_lists = [
    ("fpo-io-prod-lambda_handler", "Pure CPU"),
    ("fpo-io-prod-lambda_handler", "Pure IO"),
]

for image_type, function_type in image_function_lists:
    from_to_codes = []
    avg_runtime = []
    _50th_runtime = []
    _90th_runtime = []
    _95th_runtime = []
    _99th_runtime = []

    for region in regions:
        # From region key
        source_region = region[1] + "_" + region[0]
        from_key = image_type + '_' + source_region + '_' + function_type
        runtime_information_origin = retrieve_runtime_information(from_key)
        if runtime_information_origin is None:
            continue

        for region2 in regions:
            region_from_to_code = region[1] + ":" + region[0] + ":" + region2[1] + ":" + region2[0]

            # To region key
            destination_region = region2[1] + "_" + region2[0]
            to_key = image_type + '_' + destination_region + '_' + function_type
            runtime_information_destination = retrieve_runtime_information(to_key)
            if runtime_information_destination is None:
                continue
            

            average_percent_difference = (runtime_information_destination["average"] - runtime_information_origin["average"]) / runtime_information_origin["average"] + 1
            _50th_percent_difference = (runtime_information_destination["50th"] - runtime_information_origin["50th"]) / runtime_information_origin["50th"] + 1
            _90th_percent_difference = (runtime_information_destination["90th"] - runtime_information_origin["90th"]) / runtime_information_origin["90th"] + 1
            _95th_percent_difference = (runtime_information_destination["95th"] - runtime_information_origin["95th"]) / runtime_information_origin["95th"] + 1
            _99th_percent_difference = (runtime_information_destination["99th"] - runtime_information_origin["99th"]) / runtime_information_origin["99th"] + 1

            from_to_codes.append(region_from_to_code)
            avg_runtime.append(average_percent_difference)
            _50th_runtime.append(_50th_percent_difference)
            _90th_runtime.append(_90th_percent_difference)
            _95th_runtime.append(_95th_percent_difference)
            _99th_runtime.append(_99th_percent_difference)

        

    data = {
        "From": [code.split(":")[1] for code in from_to_codes],
        "To": [code.split(":")[3] for code in from_to_codes],
        "Avg": avg_runtime,
        "50th Percentile": _50th_runtime,
        "90th Percentile": _90th_runtime,
        "95th Percentile": _95th_runtime,
        "99th Percentile": _99th_runtime,
    }

    # print(data)

    df = pd.DataFrame(data)
    latency_columns = metrics = ["Avg", "50th Percentile", "90th Percentile", "95th Percentile", "99th Percentile"]

    df[latency_columns] = df[latency_columns].apply(pd.to_numeric, errors="coerce")

    # Create a pivot table
    pivot_df = df.pivot_table(index="From", columns="To")

    for metric in metrics:
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_df[metric], annot=True, fmt=".2f", cmap="YlGnBu", annot_kws={"size": 4})
        plt.title(f"Execution time difference of {metric} for {function_type} function")
        plt.gca().invert_xaxis()
        plt.tight_layout()

        plt.savefig(f"plots/{function_type}/runtime_{metric}.png", dpi=300)
