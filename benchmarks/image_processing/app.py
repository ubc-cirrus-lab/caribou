from typing import Any

import json
import boto3
from tempfile import TemporaryDirectory
from PIL import Image, ImageFilter
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

    if "image_name" in event:
        image_name = event["image_name"]
    else:
        raise ValueError("No image name provided")

    run_id = workflow.get_run_id()

    s3 = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:
        s3.download_file("multi-x-serverless-image-processing-benchmark", image_name, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        image_stream = BytesIO()

        image.save(image_stream, format="JPEG", quality=100)

        tmp_result_file = f"{tmp_dir}/{image_name}"

        with open(tmp_result_file, "wb") as f:
            f.write(image_stream.getvalue())

        image_name_without_extension = image_name.split(".")[0]

        new_image_name = f"{image_name_without_extension}-{run_id}.jpg"

        remote_path = f"input_images/{new_image_name}"

        s3.upload_file(tmp_result_file, "multi-x-serverless-image-processing-benchmark", remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(flip, payload)

    return {"status": 200}


@workflow.serverless_function(name="Flip")
def flip(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3")

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file("multi-x-serverless-image-processing-benchmark", path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        image_bytes = image.tobytes()

        image_stream = BytesIO(image_bytes)
        image = Image.open(image_stream)
        img = image.transpose(Image.FLIP_LEFT_RIGHT)

        image_stream = BytesIO()
        img.save(image_stream, format="JPEG", quality=50)

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        with open(tmp_result_file, "wb") as f:
            f.write(image_stream.getvalue())

        remote_path = f"flipped_images/{image_name}"

        s3.upload_file(tmp_result_file, "multi-x-serverless-image-processing-benchmark", remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(rotate, payload)

    return {"status": 200}


@workflow.serverless_function(name="Rotate")
def rotate(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3")

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file("multi-x-serverless-image-processing-benchmark", path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.transpose(Image.ROTATE_90)

        image_stream = BytesIO()
        img.save(image_stream, format="JPEG", quality=50)

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        with open(tmp_result_file, "wb") as f:
            f.write(image_stream.getvalue())

        remote_path = f"rotated_images/{image_name}"

        s3.upload_file(tmp_result_file, "multi-x-serverless-image-processing-benchmark", remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(filter_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Filter")
def filter_function(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3")

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file("multi-x-serverless-image-processing-benchmark", path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.filter(ImageFilter.BLUR)

        image_stream = BytesIO()
        img.save(image_stream, format="JPEG", quality=50)

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        with open(tmp_result_file, "wb") as f:
            f.write(image_stream.getvalue())

        remote_path = f"filtered_images/{image_name}"

        s3.upload_file(tmp_result_file, "multi-x-serverless-image-processing-benchmark", remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(greyscale, payload)

    return {"status": 200}


@workflow.serverless_function(name="Greyscale")
def greyscale(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3")

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file("multi-x-serverless-image-processing-benchmark", path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.convert("L")

        image_stream = BytesIO()
        img.save(image_stream, format="JPEG", quality=50)

        tmp_result_file = f"{tmp_dir}/result_{image_name}"

        with open(tmp_result_file, "wb") as f:
            f.write(image_stream.getvalue())

        remote_path = f"greyscale_images/{image_name}"

        s3.upload_file(tmp_result_file, "multi-x-serverless-image-processing-benchmark", remote_path)

        payload = {
            "path": remote_path,
        }

        workflow.invoke_serverless_function(resize, payload)

    return {"status": 200}


@workflow.serverless_function(name="Resize")
def resize(event: dict[str, Any]) -> dict[str, Any]:
    path = event["path"]

    s3 = boto3.client("s3")

    with TemporaryDirectory() as tmp_dir:

        image_name = path.split("/")[-1]

        s3.download_file("multi-x-serverless-image-processing-benchmark", path, f"{tmp_dir}/{image_name}")

        image = Image.open(f"{tmp_dir}/{image_name}")

        img = image.resize((128, 128))

        new_image_name = f"resize-{image_name}"

        img.save(f"{tmp_dir}/{new_image_name}", format="JPEG", quality=50)

        upload_path = f"resized_images/{new_image_name}"

        s3.upload_file(f"{tmp_dir}/{new_image_name}", "multi-x-serverless-image-processing-benchmark", upload_path)

    return {"status": 200}
