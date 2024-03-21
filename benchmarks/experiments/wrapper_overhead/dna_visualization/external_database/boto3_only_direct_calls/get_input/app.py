import boto3
import json

def get_input(event, context):
    if "gen_file_name" not in event:
        raise ValueError("No gen_file_name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")
    
    data = {
        "gen_file_name": event["gen_file_name"],
        'metadata': event['metadata']
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