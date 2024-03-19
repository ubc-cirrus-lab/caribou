import json
import boto3

def get_input(event, context):
    sns_message = json.loads(event['Records'][0]['Sns']['Message'])

    if "gen_file_name" in sns_message:
        gen_file_name = sns_message["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    if "publish_topic_arn" in sns_message:
        publish_topic_arn = sns_message["publish_topic_arn"]
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

    return {"statusCode": 200}