import boto3
import json

from src.storage.memcached import memcached_operator


def handler(event, context):

    function_name = "lambda_core"

    # dataset setting
    dataset_name = 'higgs'
    data_bucket = "higgs-10"
    dataset_type = "dense_libsvm"
    n_features = 30
    host = "127.0.0.1"
    port = 11211
    tmp_bucket = "tmp-params"
    merged_bucket = "merged-params"

    # hyper-parameters
    n_clusters = 2
    n_epochs = 10
    threshold = 0.0001

    # training setting
    sync_mode = "reduce"    # reduce or reduce_scatter
    n_workers = 10

    # lambda payload
    payload = dict()
    payload['dataset'] = dataset_name
    payload['data_bucket'] = data_bucket
    payload['dataset_type'] = dataset_type
    payload['n_features'] = n_features
    payload['tmp_bucket'] = tmp_bucket
    payload['merged_bucket'] = merged_bucket
    payload['n_clusters'] = n_clusters
    payload['n_epochs'] = n_epochs
    payload['threshold'] = threshold
    payload['sync_mode'] = sync_mode
    payload['n_workers'] = n_workers
    payload['host'] = host
    payload['port'] = port

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(n_workers):
        payload['worker_index'] = i
        payload['file'] = '{}_{}'.format(i, n_workers)
        lambda_client.invoke(FunctionName=function_name,
                             InvocationType='Event',
                             Payload=json.dumps(payload))
