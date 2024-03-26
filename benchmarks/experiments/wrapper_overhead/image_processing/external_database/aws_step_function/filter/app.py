import boto3
import tempfile
from PIL import Image, ImageFilter

def filter(event, context):
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

    return {
        "image_name": new_image_name,
        'metadata': event['metadata']
    }