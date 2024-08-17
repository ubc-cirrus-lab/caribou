import boto3
import json


def handler(event, context):
    num_workers = 10
    host = "172.31.14.3"
    port = 27000
    size = 10000

    # invoke functions
    payload = dict()
    payload['num_workers'] = num_workers
    payload['host'] = host
    payload['port'] = port
    payload['size'] = size

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(num_workers):
        payload['rank'] = i
        lambda_client.invoke(FunctionName='ps-test', InvocationType='Event', Payload=json.dumps(payload))
