import boto3
import json
import datetime
import logging 

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

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

    # Log additional information
    log_additional_info(event)

    return {"statusCode": 200}

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