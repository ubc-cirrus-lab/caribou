from typing import Any

import json
import boto3
from tempfile import TemporaryDirectory
from PIL import Image, ImageFilter

from caribou.deployment.client import CaribouWorkflow
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Change the following bucket name and region to match your setup
s3_bucket_name = "caribou-image-processing-benchmark"
s3_bucket_region_name = "us-east-1"

workflow = CaribouWorkflow(name="image_processing", version="0.0.1")

@workflow.serverless_function(
    name="get_requests",
    entry_point=True,
)
def get_requests(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "image_name" in event:
        image_name: str = event["image_name"]
    else:
        raise ValueError("No image name provided")
    if "desired_transformations" in event:
        desired_transformations: list[str] = event["desired_transformations"]
    if len(desired_transformations) > 5:
        raise ValueError("Too many transformations")
    elif len(desired_transformations) == 0:
        raise ValueError("No transformations provided")

    payload = {
        "image_name": image_name,
    }

    # Processor 1
    payload["desired_transformation"] = desired_transformations[0]
    workflow.invoke_serverless_function(image_processor, payload)

    # Processor 2
    payload["desired_transformation"] = desired_transformations[1] if len(desired_transformations) > 1 else None
    workflow.invoke_serverless_function(image_processor, payload, len(desired_transformations) > 1)

    # Processor 3
    payload["desired_transformation"] = desired_transformations[2] if len(desired_transformations) > 2 else None
    workflow.invoke_serverless_function(image_processor, payload, len(desired_transformations) > 2)

    # Processor 4
    payload["desired_transformation"] = desired_transformations[3] if len(desired_transformations) > 3 else None
    workflow.invoke_serverless_function(image_processor, payload, len(desired_transformations) > 3)
    
    # Processor 5
    payload["desired_transformation"] = desired_transformations[4] if len(desired_transformations) > 4 else None
    workflow.invoke_serverless_function(image_processor, payload, len(desired_transformations) > 4)

    return {"status": 200}

@workflow.serverless_function(
    name="image_processor",
)
def image_processor(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "image_name" in event:
        image_name = event["image_name"]
    else:
        raise ValueError("No image name provided")
    if "desired_transformation" in event:
        desired_transformation = event["desired_transformation"]

    # Download the image from S3
    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    with TemporaryDirectory() as tmp_dir:
        remote_image_name_path = f"input/{image_name}"
        s3.download_file(
            s3_bucket_name, remote_image_name_path, f"{tmp_dir}/{image_name}"
        )
        image = Image.open(f"{tmp_dir}/{image_name}")
        logger.info(f"Performing {desired_transformation} on {image_name}")
        if desired_transformation == "flip":
            img = image.transpose(Image.FLIP_LEFT_RIGHT)
            remote_path = f"output/flipped_{image_name}"
        elif desired_transformation == "rotate":
            img = image.transpose(Image.ROTATE_90)
            remote_path = f"output/rotated_{image_name}"
        elif desired_transformation == "blur":
            img = image.filter(ImageFilter.BLUR)
            remote_path = f"output/blurred_{image_name}"
        elif desired_transformation == "greyscale" or desired_transformation == "grayscale":
            img = image.convert("L")
            remote_path = f"output/greyscale_{image_name}"
        elif desired_transformation == "resize":
            img = image.resize((128, 128))
            remote_path = f"output/resized_{image_name}"
        else:
            raise ValueError("Invalid transformation")

        tmp_result_file = f"{tmp_dir}/result_{image_name}"
        img.save(tmp_result_file, format="JPEG", quality=100)
        s3.upload_file(tmp_result_file, s3_bucket_name, remote_path)

    return {"status": 200}