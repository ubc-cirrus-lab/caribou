import boto3
import json


def handler(event, context):
    dataset_name = 's3'
    bucket_name = "s3-10"
    num_workers = 10
    host = "172.31.14.3"
    port = 27000

    # invoke functions
    payload = dict()
    payload['dataset'] = dataset_name
    payload['bucket_name'] = bucket_name
    payload['num_workers'] = num_workers
    payload['host'] = host
    payload['port'] = port

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(num_workers):
        payload['rank'] = i
        payload['file'] = '{}_{}'.format(i, num_workers)
        lambda_client.invoke(FunctionName='ps-test', InvocationType='Event', Payload=json.dumps(payload))
