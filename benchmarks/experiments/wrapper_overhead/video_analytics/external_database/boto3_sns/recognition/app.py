import json
import boto3
import datetime
import logging 
import tempfile
import os
from torchvision import transforms
import torchvision.models as models
import torch
from PIL import Image
import io

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def recognition(event, context):
    event = json.loads(event['Records'][0]['Sns']['Message'])
    decoded_filename = event["decoded_filename"]
    request_id = event["request_id"]
    print(f"Recognizing video: {decoded_filename}")

    s3 = boto3.client("s3", region_name="us-west-2")
    response = s3.get_object(Bucket="multi-x-serverless-video-analytics", Key=decoded_filename)
    image_bytes = response["Body"].read()

    # Perform inference
    result = infer(image_bytes)

    # Upload the result to S3
    result_key = f"output/{request_id}-{decoded_filename}-result.txt"
    s3.put_object(Bucket="multi-x-serverless-video-analytics", Key=result_key, Body=result.encode("utf-8"))

    # Log finished time
    log_finish(event)

    # Log additional information
    log_additional_info(event)
    
    return {"status": 200, "result_key": result_key}

def infer(image_bytes):
    # Load model labels
    s3 = boto3.client("s3", region_name="us-west-2")
    response = s3.get_object(Bucket="multi-x-serverless-video-analytics", Key="imagenet_labels.txt")
    labels = response["Body"].read().decode("utf-8").splitlines()

    tmp_dir = tempfile.mkdtemp()
    os.environ['TORCH_HOME'] = tmp_dir

    # Load the model
    model = models.squeezenet1_1(pretrained=True)

    frame = preprocess_image(image_bytes)
    model.eval()
    with torch.no_grad():
        out = model(frame)
    _, indices = torch.sort(out, descending=True)
    percentages = torch.nn.functional.softmax(out, dim=1)[0] * 100

    return ",".join([f"{labels[idx]}: {percentages[idx].item()}%" for idx in indices[0][:5]]).strip()


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

def preprocess_image(image_bytes):
    img = Image.open(io.BytesIO(image_bytes))
    transform = transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    img = transform(img)
    return torch.unsqueeze(img, 0)

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