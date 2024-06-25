from typing import Any

import json
import boto3
from tempfile import TemporaryDirectory
from PIL import Image, ImageFilter

from caribou.deployment.client import CaribouWorkflow

# Change the following bucket name and region to match your setup
s3_bucket_name = "dn1-caribou-image-processing-benchmark"
s3_bucket_region_name = "us-east-1"

workflow = CaribouWorkflow(name="image_processing", version="0.0.1")


@workflow.serverless_function(
    name="Flip",
    entry_point=True,
)
def flip(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "image_name" in event:
        image_name = event["image_name"]
    else:
        raise ValueError("No image name provided")

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:
        remote_image_name_path = f"input/{image_name}"

        s3.download_file(
            s3_bucket_name, remote_image_name_path, f"{tmp_dir}/{image_name}"
        )

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.transpose(Image.FLIP_LEFT_RIGHT)

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        img.save(tmp_result_file, format="JPEG", quality=100)

        remote_path = f"flipped_images/{image_name}"

        s3.upload_file(tmp_result_file, s3_bucket_name, remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(rotate, payload)

    return {"status": 200}


@workflow.serverless_function(name="Rotate")
def rotate(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file(s3_bucket_name, path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.transpose(Image.ROTATE_90)

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        img.save(tmp_result_file, format="JPEG", quality=100)

        remote_path = f"rotated_images/{image_name}"

        s3.upload_file(tmp_result_file, s3_bucket_name, remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(filter_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Filter")
def filter_function(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file(s3_bucket_name, path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.filter(ImageFilter.BLUR)

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        img.save(tmp_result_file, format="JPEG", quality=100)

        remote_path = f"filtered_images/{image_name}"

        s3.upload_file(tmp_result_file, s3_bucket_name, remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(greyscale, payload)

    return {"status": 200}


@workflow.serverless_function(name="Greyscale")
def greyscale(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file(s3_bucket_name, path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.convert("L")

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        img.save(tmp_result_file, format="JPEG", quality=100)

        remote_path = f"greyscale_images/{image_name}"

        s3.upload_file(tmp_result_file, s3_bucket_name, remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(resize, payload)

    return {"status": 200}


@workflow.serverless_function(name="Resize")
def resize(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file(s3_bucket_name, path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.resize((128, 128))

        tmp_result_file = f"{tmp_dir}/resize-{image_name}"

        img.save(tmp_result_file, format="JPEG", quality=100)

        upload_path = f"resized_images/{image_name}"

        s3.upload_file(tmp_result_file, s3_bucket_name, upload_path)

    return {"status": 200}
