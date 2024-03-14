from typing import Any

import json
import boto3
import tempfile
from PIL import Image
import uuid

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="image_processing_light", version="0.0.1")


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

    s3.download_file("multi-x-serverless-image-processing-benchmark", image_name, f"{tmp_dir}/{image_name}")

    image = Image.open(f"{tmp_dir}/{image_name}")
    img = image.transpose(Image.FLIP_LEFT_RIGHT)

    unique_id = str(uuid.uuid4())

    new_image_name = f"flip-left-right-{unique_id}-{image_name}"
    img.save(f"{tmp_dir}/{new_image_name}")

    upload_path = f"image_processing_light/{new_image_name}"

    s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    return {"status": 200}

