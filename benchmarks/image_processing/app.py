from typing import Any

import json
import boto3
import tempfile
from PIL import Image, ImageFilter
import uuid
from io import BytesIO

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="image_processing", version="0.0.1")


@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "message" in event:
        image_name = event["message"]
    else:
        raise ValueError("No image name provided")

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless-image-processing-benchmark", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")

    image_stream = BytesIO()

    image.save(image_stream, format='JPEG', quality=50)

    image_bytes = image_stream.getvalue()

    unique_id = str(uuid.uuid4())

    image_name_without_extension = image_name.split(".")[0]

    new_image_name = f"{image_name_without_extension}-{unique_id}.jpg"

    payload = {
        "image": image_bytes,
        "image_name": new_image_name,
    }

    workflow.invoke_serverless_function(flip, payload)

    return {"status": 200}


@workflow.serverless_function(name="Flip")
def flip(event: dict[str, Any]) -> dict[str, Any]:
    image_bytes = event["image"]
    image_name = event["image_name"]

    image_stream = BytesIO(image_bytes)
    image = Image.open(image_stream)
    img = image.transpose(Image.FLIP_LEFT_RIGHT)

    new_image_name = f"flip-left-right-{image_name}"

    image_stream = BytesIO()
    img.save(image_stream, format='JPEG', quality=50)
    new_image_bytes = image_stream.getvalue()

    payload = {
        "image": new_image_bytes,
        "image_name": new_image_name,
    }

    workflow.invoke_serverless_function(rotate, payload)

    return {"status": 200}


@workflow.serverless_function(name="Rotate")
def rotate(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]
    image_bytes = event["image"]

    image_stream = BytesIO(image_bytes)
    image = Image.open(image_stream)
    img = image.transpose(Image.ROTATE_90)

    new_image_name = f"rotate-90-{image_name}"

    image_stream = BytesIO()
    img.save(image_stream, format='JPEG', quality=50)
    new_image_bytes = image_stream.getvalue()

    payload = {
        "image": new_image_bytes,
        "image_name": new_image_name,
    }

    workflow.invoke_serverless_function(filter_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Filter")
def filter_function(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]
    image_bytes = event["image"]

    image_stream = BytesIO(image_bytes)
    image = Image.open(image_stream)
    img = image.filter(ImageFilter.BLUR)

    new_image_name = f"filter-{image_name}"

    image_stream = BytesIO()
    img.save(image_stream, format='JPEG', quality=50)
    new_image_bytes = image_stream.getvalue()

    payload = {
        "image": new_image_bytes,
        "image_name": new_image_name,
    }

    workflow.invoke_serverless_function(greyscale, payload)

    return {"status": 200}


@workflow.serverless_function(name="Greyscale")
def greyscale(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]
    image_bytes = event["image"]

    image_stream = BytesIO(image_bytes)
    image = Image.open(image_stream)
    img = image.convert("L")

    image_stream = BytesIO()
    img.save(image_stream, format='JPEG', quality=50)
    new_image_bytes = image_stream.getvalue()

    new_image_name = f"greyscale-{image_name}"

    payload = {
        "image": new_image_bytes,
        "image_name": new_image_name,
    }

    workflow.invoke_serverless_function(resize, payload)

    return {"status": 200}


@workflow.serverless_function(name="Resize")
def resize(event: dict[str, Any]) -> dict[str, Any]:
    image_name = event["image_name"]
    image_bytes = event["image"]

    image_stream = BytesIO(image_bytes)
    image = Image.open(image_stream)
    img = image.resize((128, 128))
    new_image_name = f"resize-{image_name}"

    s3 = boto3.client("s3")

    tmp_dir = tempfile.mkdtemp()

    img.save(f"{tmp_dir}/{new_image_name}", format='JPEG', quality=50)

    upload_path = f"image_processing/{new_image_name}"

    s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    return {"status": 200}
