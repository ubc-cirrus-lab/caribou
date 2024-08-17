import boto3
import json

from src.storage.s3 import s3_operator


def handler(event, context):

    function_name = "lambda_core"

    # dataset setting
    dataset_name = 'higgs'
    data_bucket = "higgs-10"
    dataset_type = "dense_libsvm"   # dense_libsvm or sparse_libsvm
    n_features = 30
    n_classes = 2

    # ps setting
    host = "127.0.0.1"
    port = 27000

    # training setting
    model = "lr"    # lr, svm, sparse_lr, or sparse_svm
    optim = "grad_avg"  # grad_avg
    sync_mode = "reduce"    # reduce
    n_workers = 10

    # hyper-parameters
    lr = 0.01
    batch_size = 100000
    n_epochs = 2
    valid_ratio = .2

    # lambda payload
    payload = dict()
    payload['dataset'] = dataset_name
    payload['data_bucket'] = data_bucket
    payload['dataset_type'] = dataset_type
    payload['n_features'] = n_features
    payload['n_classes'] = n_classes
    payload['host'] = host
    payload['port'] = port
    payload['n_workers'] = n_workers
    payload['model'] = model
    payload['optim'] = optim
    payload['sync_mode'] = sync_mode
    payload['lr'] = lr
    payload['batch_size'] = batch_size
    payload['n_epochs'] = n_epochs
    payload['valid_ratio'] = valid_ratio

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(n_workers):
        payload['worker_index'] = i
        payload['file'] = '{}_{}'.format(i, n_workers)
        lambda_client.invoke(FunctionName=function_name,
                             InvocationType='Event',
                             Payload=json.dumps(payload))
