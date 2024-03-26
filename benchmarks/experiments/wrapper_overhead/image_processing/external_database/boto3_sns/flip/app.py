import tempfile
import boto3
import json

import uuid

from PIL import Image


def flip(event, context):
    event = json.loads(event['Records'][0]['Sns']['Message'])
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

    payload = {
        "image_name": new_image_name,
        'metadata': event['metadata']
    }

    # Convert the data to JSON
    payload = json.dumps(payload)

    send_sns("wo-im_p-ed-sns-rotate", payload)

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