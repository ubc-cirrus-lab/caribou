import tempfile
import boto3
import json

from PIL import Image
import logging 

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def rotate(event, context):
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
    
    # Convert the data to JSON
    payload = json.dumps(payload)

    # Invoke the next visualize function
    # With boto3 only, the function is invoked directly
    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName="wo-im_p-ed-direct-filter", # Name of next function
        InvocationType="Event",
        Payload=payload,
    )

    # Log additional information
    log_additional_info(event)

    return {"statusCode": 200}

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