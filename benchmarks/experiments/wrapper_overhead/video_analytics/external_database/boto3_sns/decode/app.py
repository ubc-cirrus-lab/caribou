import boto3
import json
import logging 
import cv2
import tempfile
import os

FANOUT_NUM = 4

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def decode(event, context):
    event = json.loads(event['Records'][0]['Sns']['Message'])
    video_name = event["video_name"]
    request_id = event["request_id"]
    print(f"Decoding video: {video_name}")

    decoded_filename = video_analytics_decode(video_name, request_id)

    payload = {
        "decoded_filename": decoded_filename,
        "request_id": request_id,
        'metadata': event['metadata']
    }

    send_sns("wo-vid_an-ed-sns-recognition", json.dumps(payload))

    # Log additional information
    log_additional_info(event)

    return {"statusCode": 200}

def video_analytics_decode(filename: str, request_id: int) -> str:
    tmp_dir = tempfile.mkdtemp()
    local_filename = os.path.join(tmp_dir, filename)

    # Make sure the directory exists
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)

    s3 = boto3.client("s3")

    # Download the video file from S3
    s3.download_file("multi-x-serverless-video-analytics", filename, local_filename)

    # Open the video file
    cap = cv2.VideoCapture(local_filename)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Calculate frame range for this request_id
    frames_per_section = total_frames // FANOUT_NUM
    start_frame = frames_per_section * (request_id - 1)
    end_frame = start_frame + frames_per_section

    # Adjust for the last section to cover all remaining frames
    if request_id == FANOUT_NUM:
        end_frame = total_frames

    # Seek to the start frame
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    # Process and upload frames from start_frame to end_frame
    for frame_idx in range(start_frame, end_frame):
        success, image = cap.read()
        if not success:
            break  # Exit loop if no frame is found

        frame_name = filename.split(".")[0]

        decoded_filename = f"output/decoded-{request_id}-{frame_idx}-{frame_name}.jpg"
        decoded_local_path = os.path.join(tmp_dir, decoded_filename)

        # Make sure the directory exists
        os.makedirs(os.path.dirname(decoded_local_path), exist_ok=True)

        # Write the frame to a temporary file
        cv2.imwrite(decoded_local_path, image)

        # Upload the frame to S3
        s3.upload_file(decoded_local_path, "multi-x-serverless-video-analytics", decoded_filename)

    # Return the name of the last uploaded frame as an example
    return decoded_filename


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