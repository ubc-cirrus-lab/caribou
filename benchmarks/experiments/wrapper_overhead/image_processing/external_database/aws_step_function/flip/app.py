import tempfile
import boto3
import uuid

from PIL import Image


def flip(event, context):
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

    return {
        "image_name": new_image_name,
        'metadata': event['metadata']
    }