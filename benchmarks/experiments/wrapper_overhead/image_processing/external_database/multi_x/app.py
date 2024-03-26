import datetime
from typing import Any

import json
import boto3
import tempfile
from PIL import Image, ImageFilter
import uuid

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow
import logging 

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

workflow = MultiXServerlessWorkflow(name="wo-im_p-ed-multi_x", version="0.0.1")

@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
    
    if isinstance(event, str):
        event = json.loads(event)

    if "message" not in event:
        raise ValueError("No image name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")

    # Get the additional metadata
    metadata = event['metadata']
    metadata['first_function_start_time'] = start_time

    payload = {
        "image_name": event["message"],
        'metadata': metadata
    }

    workflow.invoke_serverless_function(flip, payload)

    return {"status": 200}


@workflow.serverless_function(name="Flip")
def flip(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless-image-processing-benchmark", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.transpose(Image.FLIP_LEFT_RIGHT)

    unique_id = str(uuid.uuid4())

    new_image_name = f"flip-left-right-{unique_id}-{image_name}"
    img.save(f"{tmp_dir}/{new_image_name}")

    upload_path = f"image_processing/{new_image_name}"

    s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    payload = {
        "image_name": new_image_name,
        'metadata': event['metadata']
    }

    workflow.invoke_serverless_function(rotate, payload)

    return {"status": 200}


@workflow.serverless_function(name="Rotate")
def rotate(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file(
        "multi-x-serverless-image-processing-benchmark", f"image_processing/{image_name}", f"{tmp_dir}/{image_name}"
    )

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.transpose(Image.ROTATE_90)

    new_image_name = f"rotate-90-{image_name}"
    img.save(f"{tmp_dir}/{new_image_name}")

    upload_path = f"image_processing/{new_image_name}"

    s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    payload = {
        "image_name": new_image_name,
        'metadata': event['metadata']
    }

    workflow.invoke_serverless_function(filter_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Filter")
def filter_function(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file(
        "multi-x-serverless-image-processing-benchmark", f"image_processing/{image_name}", f"{tmp_dir}/{image_name}"
    )

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.filter(ImageFilter.BLUR)

    new_image_name = f"filter-{image_name}"
    img.save(f"{tmp_dir}/{new_image_name}")

    upload_path = f"image_processing/{new_image_name}"

    s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    payload = {
        "image_name": new_image_name,
        'metadata': event['metadata']
    }

    workflow.invoke_serverless_function(greyscale, payload)

    return {"status": 200}


@workflow.serverless_function(name="Greyscale")
def greyscale(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file(
        "multi-x-serverless-image-processing-benchmark", f"image_processing/{image_name}", f"{tmp_dir}/{image_name}"
    )

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.convert("L")

    new_image_name = f"greyscale-{image_name}"
    img.save(f"{tmp_dir}/{new_image_name}")

    upload_path = f"image_processing/{new_image_name}"

    s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    payload = {
        "image_name": new_image_name,
        'metadata': event['metadata']
    }

    workflow.invoke_serverless_function(resize, payload)

    return {"status": 200}


@workflow.serverless_function(name="Resize")
def resize(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file(
        "multi-x-serverless-image-processing-benchmark", f"image_processing/{image_name}", f"{tmp_dir}/{image_name}"
    )

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.resize((128, 128))
    new_image_name = f"resize-{image_name}"
    img.save(f"{tmp_dir}/{new_image_name}")

    upload_path = f"image_processing/{new_image_name}"

    s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    log_finish(event) # Log finished time

    return {"status": 200}

def log_finish(event):
    # Log the end time of the function
    ## Get the current time
    current_time = datetime.datetime.now(datetime.timezone.utc)
    final_function_end_time = current_time.strftime("%Y-%m-%d %H:%M:%S,%f%z")

    ## Get the start time from the metadata
    start_time_str = event["metadata"]["start_time"]
    start_time = datetime.datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S,%f%z")

    first_function_start_time_str = event["metadata"]["first_function_start_time"]
    first_function_start_time = datetime.datetime.strptime(first_function_start_time_str, "%Y-%m-%d %H:%M:%S,%f%z")
    
    ## Calculate the time delta in miliseconds
    ### For the time taken from the perspective of customer
    time_difference = current_time - start_time
    ms_from_start = (time_difference.days * 24 * 3600 * 1000) + (time_difference.seconds * 1000) + (time_difference.microseconds / 1000)

    ### For the time taken from the first function running
    time_difference = current_time - first_function_start_time
    ms_from_first_function = (time_difference.days * 24 * 3600 * 1000) + (time_difference.seconds * 1000) + (time_difference.microseconds / 1000)

    ## Get the workload name from the metadata
    workload_name = event["metadata"]["workload_name"]
    request_id = event["metadata"]["request_id"]

    ## Log the time taken along with the request ID and workload name
    logger.info(f"Workload Name: {workload_name}, "
                f"Request ID: {request_id}, "
                f"Client Start Time: {start_time_str}, "
                f"First Function Start Time: {first_function_start_time_str}, "
                f"Time Taken from workload invocation from client: {ms_from_start} ms, "
                f"Time Taken from first function: {ms_from_first_function} ms, "
                f"Function End Time: {final_function_end_time}")