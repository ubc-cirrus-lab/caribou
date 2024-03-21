import datetime
import json
import boto3

def get_input(event, context):
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
    event = json.loads(event['Records'][0]['Sns']['Message'])
    if "metadata" not in event:
        raise ValueError("No metadata provided")
    if "gen_file_name" not in event:
        raise ValueError("No gen_file_name provided")

    # Get the additional metadata
    metadata = event['metadata']
    metadata['first_function_start_time'] = start_time    
    data = {
        "gen_file_name": event["gen_file_name"],
        'metadata': metadata
    }

    payload = json.dumps(data)

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