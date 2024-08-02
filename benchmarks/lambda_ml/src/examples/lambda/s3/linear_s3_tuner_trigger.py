import boto3
import json
import time

from src.storage.s3 import s3_operator
from src.storage import DynamoTable
from src.storage.dynamo import dynamo_operator

from tuner.config.hyper_space import ContHyper, DiscHyper


def handler(event, context):

    tuner_function_name = "lambda_tuner"
    trial_function_name = "lambda_trial"
    function_start = time.time()
    function_duration = 14 * 60
    n_submit_trial = event.get('n_submit_trial', 0)

    # dataset setting
    dataset_name = 'higgs'
    data_bucket = "higgs-10"
    dataset_type = "dense_libsvm"   # dense_libsvm or sparse_libsvm
    n_features = 30
    n_classes = 2
    tmp_bucket = "tmp-params"
    merged_bucket = "merged-params"

    # training setting
    model = "lr"    # lr, svm, sparse_lr, or sparse_svm
    optim = "grad_avg"  # grad_avg, model_avg, or admm
    sync_mode = "reduce"    # async, reduce or reduce_scatter
    n_workers = 10

    # tuner configs
    tuner_strategy = "random_search"
    tuner_concurrency = 5
    lr_values = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
    lr_disc = DiscHyper("lr_discrete", lr_values)

    # hyper-parameters
    lr = 0.01
    batch_size = 100000
    n_epochs = 2
    valid_ratio = .2
    n_admm_epochs = 2
    lam = 0.01
    rho = 0.01

    # clear s3 bucket
    s3_client = s3_operator.get_client()
    s3_operator.clear_bucket(s3_client, tmp_bucket)
    s3_operator.clear_bucket(s3_client, merged_bucket)

    # set dynamodb table
    recorder_table_name = "recoder"
    dynamo_client = dynamo_operator.get_client()
    recorder_tb = DynamoTable(dynamo_client, recorder_table_name)
    items = recorder_tb.list()
    print("{} items in the recorder".format(len(items)))

    # lambda payload
    payload = dict()
    payload['dataset'] = dataset_name
    payload['data_bucket'] = data_bucket
    payload['dataset_type'] = dataset_type
    payload['n_features'] = n_features
    payload['n_classes'] = n_classes
    payload['n_workers'] = n_workers
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

    n_trial = 10
    trial_counter = n_submit_trial

    for i in range(n_trial):
        n_recorder_items = len(recorder_tb.list())
        n_running_tail = trial_counter - n_recorder_items
        while n_running_tail >= tuner_concurrency:
            time.sleep(1)
            n_recorder_items = len(recorder_tb.list())
            n_running_tail = trial_counter - n_recorder_items
        for j in range(n_workers):
            payload = dict()
            payload['dataset'] = dataset_name
            payload['data_bucket'] = data_bucket
            payload['dataset_type'] = dataset_type
            payload['n_features'] = n_features
            payload['n_classes'] = n_classes
            payload['n_workers'] = n_workers
            payload['tmp_bucket'] = tmp_bucket
            payload['merged_bucket'] = merged_bucket
            payload['model'] = model
            payload['optim'] = optim
            payload['sync_mode'] = sync_mode
            payload['batch_size'] = batch_size
            payload['n_epochs'] = n_epochs
            payload['valid_ratio'] = valid_ratio
            payload['n_admm_epochs'] = n_admm_epochs
            payload['lambda'] = lam
            payload['rho'] = rho
            payload['function_name'] = trial_function_name

            payload['tmp_bucket'] = tmp_bucket + "-i"
            payload['merged_bucket'] = merged_bucket + "-i"
            payload['lr'] = lr_disc.next() if tuner_strategy == "grid_search" else lr_disc.sample()
            payload['worker_index'] = j
            payload['train_file'] = 'training_{}.pt'.format(j)
            payload['test_file'] = 'test.pt'
            lambda_client.invoke(FunctionName=trial_function_name,
                                 InvocationType='Event',
                                 Payload=json.dumps(payload))
        trial_counter += 1
        if time.time() - function_start > function_duration:
            # revoke itself
            print("Invoking the next round of tuner functions, total trials {}, submitted trials {}"
                  .format(n_trial, trial_counter))
            lambda_client = boto3.client('lambda')
            payload = {
                'n_submit_trial': n_submit_trial
            }
            lambda_client.invoke(FunctionName=tuner_function_name,
                                 InvocationType='Event',
                                 Payload=json.dumps(payload))
