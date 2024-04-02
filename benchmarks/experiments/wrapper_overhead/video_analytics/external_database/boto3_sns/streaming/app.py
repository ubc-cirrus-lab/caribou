import boto3
import json
import datetime
import logging 
import tempfile
import os
import cv2

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def streaming(event, context):
    event = json.loads(event['Records'][0]['Sns']['Message'])
    video_name = event["video_name"]
    request_id = event["request_id"]
    print(f"Processing video: {video_name}")

    video_name = video_analytics_streaming(video_name, request_id)

    payload = {
        "video_name": video_name,
        "request_id": request_id,
        'metadata': event['metadata']
    }

    send_sns("wo-vid_an-ed-sns-decode", json.dumps(payload))

    # Log additional information
    log_additional_info(event)

    return {"statusCode": 200}

def video_analytics_streaming(filename: str, request_id: int) -> str:
    tmp_dir = tempfile.mkdtemp()
    local_filename = os.path.join(tmp_dir, filename)

    # Make sure the directory exists
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)

    s3 = boto3.client("s3")

    s3.download_file("multi-x-serverless-video-analytics", filename, local_filename)

    resized_local_filename = resize_and_store(local_filename)

    streaming_filename = f"output/streaming-{request_id}-{filename}"

    s3.upload_file(resized_local_filename, "multi-x-serverless-video-analytics", streaming_filename)

    return streaming_filename

def resize_and_store(local_filename: str) -> str:
    tmp_dir = tempfile.mkdtemp()
    cap = cv2.VideoCapture(local_filename)
    fps = cap.get(cv2.CAP_PROP_FPS)
    fourcc = cv2.VideoWriter_fourcc("m", "p", "4", "v")
    resized_local_filename = os.path.join(tmp_dir, "resized.mp4")
    width, height = 340, 256
    writer = cv2.VideoWriter(resized_local_filename, fourcc, fps, (width, height))

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        frame = cv2.resize(frame, (width, height))
        writer.write(frame)
    return resized_local_filename


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