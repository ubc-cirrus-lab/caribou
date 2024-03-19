import boto3
import json

def get_input(event, context):
    # Read the payload from the event and parse the JSON string to a Python dictionary
    parsed_event = json.loads(event["Payload"])

    if "gen_file_name" in parsed_event:
        gen_file_name = parsed_event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    data = {
        "gen_file_name": gen_file_name,
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