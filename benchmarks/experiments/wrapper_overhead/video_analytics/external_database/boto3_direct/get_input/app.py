import boto3
import json
import datetime
import logging 

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def get_input(event, context):
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
    if isinstance(event, str):
        event = json.loads(event)

    if "message" in event:
        video_name = event["message"]
    else:
        raise ValueError("No message provided")

    # Get the additional metadata
    metadata = event['metadata']
    metadata['first_function_start_time'] = start_time

    payload = {
        "video_name": video_name,
        "request_id": 1,
        'metadata': metadata
    }
    invoke_lambda_function("wo-vid_an-ed-direct-streaming", json.dumps(payload))

    payload["request_id"] = 2
    invoke_lambda_function("wo-vid_an-ed-direct-streaming", json.dumps(payload))

    payload["request_id"] = 3
    invoke_lambda_function("wo-vid_an-ed-direct-streaming", json.dumps(payload))

    payload["request_id"] = 4
    invoke_lambda_function("wo-vid_an-ed-direct-streaming", json.dumps(payload))

    # Log additional information
    log_additional_info(event)

    return {"statusCode": 200}

def invoke_lambda_function(function_name, payload):
    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=payload,
    )

def log_additional_info(event):
    # Log the CPU model, workflow name, and the request ID
    cpu_model = ""
    with open('/proc/cpuinfo') as f:
        for line in f:
            if "model name" in line:
                cpu_model = line.split(":")[1].strip()  # Extracting and cleaning the model name
                break  # No need to continue the loop once the model name is found

    workload_name = event["metadata"]["workload_name"]
    request_id = event["metadata"]["request_id"]

    logger.info(f"Workload Name: {workload_name}, Request ID: {request_id}, CPU Model: {cpu_model}")