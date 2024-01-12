from typing import Any

import json
import boto3
import tempfile
from PIL import Image, ImageFilter
import uuid

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow("image_processing")


@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
    regions_and_providers={
        "only_regions": [
            {
                "provider": "aws",
                "region": "us-east-1",
            }
        ],
        "forbidden_regions": [
            {
                "provider": "aws",
                "region": "us-east-2",
            }
        ],
        "providers": [
            {
                "name": "aws",
                "config": {
                    "timeout": 60,
                    "memory": 128,
                },
            }
        ],
    },
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    request = event["request"]
    request_json = json.loads(request)

    if "message" in request_json:
        image_name = request_json["message"]
    else:
        raise ValueError("No image name provided")

    payload = {
        "image_name": image_name,
    }

    workflow.invoke_serverless_function(flip, payload)

    return {"status": 200}


@workflow.serverless_function(name="Flip")
def flip(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.transpose(Image.FLIP_LEFT_RIGHT)
    img.save(f"{tmp_dir}/flip-left-right-{image_name}")

    unique_id = str(uuid.uuid4())
    upload_path = f"image_processing/flip-left-right-{unique_id}-{image_name}"

    s3.upload_file(f"{tmp_dir}/flip-left-right-{image_name}", "multi-x-serverless", upload_path)

    payload = {
        "image_name": upload_path,
    }

    workflow.invoke_serverless_function(rotate, payload)

    return {"status": 200}


@workflow.serverless_function(name="Rotate")
def rotate(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.transpose(Image.ROTATE_90)
    img.save(f"{tmp_dir}/rotate-90-{image_name}")

    unique_id = str(uuid.uuid4())
    upload_path = f"image_processing/rotate-90-{unique_id}-{image_name}"

    s3.upload_file(f"{tmp_dir}/rotate-90-{image_name}", "multi-x-serverless", upload_path)

    payload = {
        "image_name": upload_path,
    }

    workflow.invoke_serverless_function(filter_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Filter")
def filter_function(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.filter(ImageFilter.BLUR)
    img.save(f"{tmp_dir}/filter-{image_name}")

    unique_id = str(uuid.uuid4())
    upload_path = f"image_processing/filter-{unique_id}-{image_name}"

    s3.upload_file(f"{tmp_dir}/filter-{image_name}", "multi-x-serverless", upload_path)

    payload = {
        "image_name": upload_path,
    }

    workflow.invoke_serverless_function(greyscale, payload)

    return {"status": 200}


@workflow.serverless_function(name="Greyscale")
def greyscale(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.convert("L")
    img.save(f"{tmp_dir}/greyscale-{image_name}")

    unique_id = str(uuid.uuid4())
    upload_path = f"image_processing/greyscale-{unique_id}-{image_name}"

    s3.upload_file(f"{tmp_dir}/greyscale-{image_name}", "multi-x-serverless", upload_path)

    payload = {
        "image_name": upload_path,
    }

    workflow.invoke_serverless_function(resize, payload)

    return {"status": 200}


@workflow.serverless_function(name="Resize")
def resize(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.resize((128, 128))
    img.save(f"{tmp_dir}/resize-{image_name}")

    unique_id = str(uuid.uuid4())
    upload_path = f"image_processing/resize-{unique_id}-{image_name}"

    s3.upload_file(f"{tmp_dir}/resize-{image_name}", "multi-x-serverless", upload_path)

    return {"status": 200}