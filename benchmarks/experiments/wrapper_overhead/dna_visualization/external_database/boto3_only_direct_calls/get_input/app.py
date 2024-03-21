import boto3
import json
import datetime

def get_input(event, context):
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
    if "gen_file_name" not in event:
        raise ValueError("No gen_file_name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")

    # Get the additional metadata
    metadata = event['metadata']
    metadata['first_function_start_time'] = start_time
    data = {
        "gen_file_name": event["gen_file_name"],
        'metadata': metadata
    }

    # Convert the data to JSON
    payload = json.dumps(data)

    # Invoke the next visualize function
    # With boto3 only, the function is invoked directly
    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName="wo-dna_vis-ed-direct-visualize", # Name of next function
        InvocationType="Event",
        Payload=payload,
    )

    return {"statusCode": 200}