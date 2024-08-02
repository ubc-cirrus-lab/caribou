import boto3
import json

from src.storage import DynamoTable
from src.storage.dynamo import dynamo_operator


def handler(event, context):

    function_name = "lambda_core"

    # dataset setting
    dataset_name = 'higgs'
    data_bucket = "higgs-10"
    dataset_type = "dense_libsvm"
    n_features = 30
    tmp_table_name = "tmp-params"
    merged_table_name = "merged-params"
    key_col = "key"

    # hyper-parameters
    n_clusters = 10
    n_epochs = 10
    threshold = 0.0001

    # training setting
    sync_mode = "reduce"    # reduce or reduce_scatter
    n_workers = 10

    # clear dynamodb table
    dynamo_client = dynamo_operator.get_client()
    tmp_tb = DynamoTable(dynamo_client, tmp_table_name)
    merged_tb = DynamoTable(dynamo_client, tmp_table_name)
    tmp_tb.clear(key_col)
    merged_tb.clear(key_col)

    # lambda payload
    payload = dict()
    payload['dataset'] = dataset_name
    payload['data_bucket'] = data_bucket
    payload['dataset_type'] = dataset_type
    payload['n_features'] = n_features
    payload['tmp_table_name'] = tmp_table_name
    payload['merged_table_name'] = merged_table_name
    payload['key_col'] = key_col
    payload['n_clusters'] = n_clusters
    payload['n_epochs'] = n_epochs
    payload['threshold'] = threshold
    payload['sync_mode'] = sync_mode
    payload['n_workers'] = n_workers

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(n_workers):
        payload['worker_index'] = i
        payload['file'] = '{}_{}'.format(i, n_workers)
        lambda_client.invoke(FunctionName=function_name,
                             InvocationType='Event',
                             Payload=json.dumps(payload))
