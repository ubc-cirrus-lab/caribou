import boto3
import json


def handler(event, context):
    num_workers = 5

    # invoke functions
    payload = dict()
    payload['num_workers'] = num_workers

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(num_workers):
        payload['rank'] = i
        lambda_client.invoke(FunctionName='ps-test', InvocationType='Event', Payload=json.dumps(payload))
