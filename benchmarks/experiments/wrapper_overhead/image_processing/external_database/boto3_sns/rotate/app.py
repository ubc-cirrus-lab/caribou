import tempfile
import boto3
import json
import logging 

from PIL import Image

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def rotate(event, context):
    event = json.loads(event['Records'][0]['Sns']['Message'])
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

    # Send the payload to the next function
    send_sns("wo-im_p-ed-sns-filter", payload)

    # Log additional information
    log_additional_info(event)

    return {"statusCode": 200}

def send_sns(next_function_name, payload):
    # Invoke the next visualize function
    # With boto3 only, using sns
    sns_topic_name = f"{next_function_name}-sns_topic"
    
    # Create an SNS client
    sns_client = boto3.client('sns')

    # Get the list of topics
    topics_response = sns_client.list_topics()

    # Get the ARN of the SNS topic
    # Find the ARN for your topic
    topic_arn = None
    for topic in topics_response['Topics']:
        if sns_topic_name in topic['TopicArn']:
            topic_arn = topic['TopicArn']
            break
    
    if topic_arn is None:
        raise ValueError("No topic found")
    
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=topic_arn,
        Message=payload
    )

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