import boto3
import json
import datetime

def get_input(event, context):
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
    event = json.loads(event['Records'][0]['Sns']['Message'])

    if "message" not in event:
        raise ValueError("No image name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")

    # Get the additional metadata
    metadata = event['metadata']
    metadata['first_function_start_time'] = start_time

    payload = {
        "image_name": event["message"],
        'metadata': metadata
    }
    
    # Convert the data to JSON
    payload = json.dumps(payload)

    send_sns("wo-im_p-ed-sns-flip", payload)

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