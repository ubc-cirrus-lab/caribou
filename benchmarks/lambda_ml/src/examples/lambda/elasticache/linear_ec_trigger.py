import boto3
import json

from src.storage.memcached import memcached_operator


def handler(event, context):

    function_name = "lambda_core"

    # dataset setting
    dataset_name = 'higgs'
    data_bucket = "higgs-10"
    dataset_type = "dense_libsvm"   # dense_libsvm or sparse_libsvm
    n_features = 30
    n_classes = 2
    host = "127.0.0.1"
    port = 11211
    tmp_bucket = "tmp-params"
    merged_bucket = "merged-params"

    # training setting
    model = "lr"    # lr, svm, sparse_lr, or sparse_svm
    optim = "grad_avg"  # grad_avg, model_avg, or admm
    sync_mode = "reduce"    # async, reduce or reduce_scatter
    n_workers = 10

    # hyper-parameters
    lr = 0.01
    batch_size = 100000
    n_epochs = 2
    valid_ratio = .2
    n_admm_epochs = 2
    lam = 0.01
    rho = 0.01

    # lambda payload
    payload = dict()
    payload['dataset'] = dataset_name
    payload['data_bucket'] = data_bucket
    payload['dataset_type'] = dataset_type
    payload['n_features'] = n_features
    payload['n_classes'] = n_classes
    payload['n_workers'] = n_workers
    payload['host'] = host
    payload['port'] = port
    payload['tmp_bucket'] = tmp_bucket
    payload['merged_bucket'] = merged_bucket
    payload['model'] = model
    payload['optim'] = optim
    payload['sync_mode'] = sync_mode
    payload['lr'] = lr
    payload['batch_size'] = batch_size
    payload['n_epochs'] = n_epochs
    payload['valid_ratio'] = valid_ratio
    payload['n_admm_epochs'] = n_admm_epochs
    payload['lambda'] = lam
    payload['rho'] = rho

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(n_workers):
        payload['worker_index'] = i
        payload['file'] = '{}_{}'.format(i, n_workers)
        lambda_client.invoke(FunctionName=function_name,
                             InvocationType='Event',
                             Payload=json.dumps(payload))
