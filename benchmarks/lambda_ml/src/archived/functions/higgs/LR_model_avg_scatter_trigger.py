import boto3
import json

from archived.sync import clear_bucket


def handler(event, context):
    dataset_name = 's3'
    bucket_name = "s3-10"
    num_workers = 10
    tmp_bucket = "tmp-params"
    merged_bucket = "merged-params"

    clear_bucket(tmp_bucket)
    clear_bucket(merged_bucket)

    # invoke functions
    payload = dict()
    payload['dataset'] = dataset_name
    payload['bucket_name'] = bucket_name
    payload['num_workers'] = num_workers
    payload['tmp_bucket'] = tmp_bucket
    payload['merged_bucket'] = merged_bucket

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(num_workers):
        payload['rank'] = i
        payload['file'] = '{}_{}'.format(i, num_workers)
        lambda_client.invoke(FunctionName='LR_higgs', InvocationType='Event', Payload=json.dumps(payload))
