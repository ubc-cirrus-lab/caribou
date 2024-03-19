import json
import boto3

def get_input(event, context):
    if "gen_file_name" in event:
        gen_file_name = event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    if "publish_topic_arn" in event:
        publish_topic_arn = event["publish_topic_arn"]
    else:
        raise ValueError("No ARN for publish provided")

    payload = {
        "gen_file_name": gen_file_name,
    }

    # Invoke the next visualize function
    # With boto3 only, using sns
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=publish_topic_arn,
        Message=payload,
        MessageStructure='json'
    )

    return {"status": 200}