import boto3
import logging 
import tempfile
import os
import cv2

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def streaming(event, context):
    video_name = event["video_name"]
    request_id = event["request_id"]

    logger.info(f"Processing video: {video_name}, Request ID: {request_id}")

    video_name = video_analytics_streaming(video_name, request_id)

    # Log additional information
    log_additional_info(event)

    return {
        "video_name": video_name,
        "request_id": request_id,
        'metadata': event['metadata']
    }

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