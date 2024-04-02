import boto3
import json
import datetime
import logging 

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def get_input(event, context):
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
    event = json.loads(event['Records'][0]['Sns']['Message'])
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
    send_sns("wo-vid_an-ed-sns-streaming", json.dumps(payload))

    payload["request_id"] = 2
    send_sns("wo-vid_an-ed-sns-streaming", json.dumps(payload))

    payload["request_id"] = 3
    send_sns("wo-vid_an-ed-sns-streaming", json.dumps(payload))

    payload["request_id"] = 4
    send_sns("wo-vid_an-ed-sns-streaming", json.dumps(payload))

    # Log additional information
    log_additional_info(event)

    return {"statusCode": 200}

def send_sns(next_function_name, payload):
    # Invoke the next visualize function
    # With boto3 only, using sns
    sns_topic_name = f"{next_function_name}-sns_topic"
    
    # Create an SNS client
    sns_client = boto3.client('sns')

    # Get the list of topics
    topics_response = sns_client.list_topics()

    # Get the ARN of the SNS topic
    # Find the ARN for your topic
    topic_arn = None
    for topic in topics_response['Topics']:
        if sns_topic_name in topic['TopicArn']:
            topic_arn = topic['TopicArn']
            break
    
    if topic_arn is None:
        raise ValueError("No topic found")
    
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=topic_arn,
        Message=payload
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