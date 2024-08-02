import boto3

import json

from archived.sync import clear_bucket


def handler(event, context):
    dataset_name = 's3'
    bucket_name = "s3-libsvm"
    num_workers = 10
    num_buckets = 1
    tmp_bucket_prefix = "tmp-params"
    merged_bucket_prefix = "merged-params"

    for i in range(num_buckets):
        clear_bucket("{}-{}".format(tmp_bucket_prefix, i))
        clear_bucket("{}-{}".format(merged_bucket_prefix, i))

    # invoke functions
    payload = dict()
    payload['dataset'] = dataset_name
    payload['bucket_name'] = bucket_name
    payload['num_workers'] = num_workers
    payload['num_buckets'] = num_buckets
    payload['tmp_bucket_prefix'] = tmp_bucket_prefix
    payload['merged_bucket_prefix'] = merged_bucket_prefix

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(num_workers):
        payload['rank'] = i
        payload['file'] = '{}_{}'.format(i, num_workers)
        lambda_client.invoke(FunctionName='LR_higgs_multibucket', InvocationType='Event', Payload=json.dumps(payload))
