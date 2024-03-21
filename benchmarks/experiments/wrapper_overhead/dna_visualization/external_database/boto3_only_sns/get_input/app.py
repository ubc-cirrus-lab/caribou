import json
import boto3

def get_input(event, context):
    sns_message = json.loads(event['Records'][0]['Sns']['Message'])

    if "gen_file_name" in sns_message:
        gen_file_name = sns_message["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    payload = {
        "gen_file_name": gen_file_name,
    }

    payload = json.dumps(payload)

    # Invoke the next visualize function
    # With boto3 only, using sns
    sns_topic_name = "wo-dna_vis-ed-direct_sns-visualize-sns_topic"
    
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

    return {"statusCode": 200}