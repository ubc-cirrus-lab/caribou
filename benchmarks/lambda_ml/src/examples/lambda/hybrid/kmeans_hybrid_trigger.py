import boto3
import json


def handler(event, context):

    function_name = "lambda_core"

    # dataset setting
    dataset_name = 'higgs'
    data_bucket = "higgs-10"
    dataset_type = "dense_libsvm"
    n_features = 30

    # ps setting
    host = "127.0.0.1"
    port = 27000

    # hyper-parameters
    n_clusters = 10
    n_epochs = 10
    threshold = 0.0001

    # training setting
    sync_mode = "reduce"    # reduce
    n_workers = 10

    # lambda payload
    payload = dict()
    payload['dataset'] = dataset_name
    payload['data_bucket'] = data_bucket
    payload['dataset_type'] = dataset_type
    payload['n_features'] = n_features
    payload['host'] = host
    payload['port'] = port
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
