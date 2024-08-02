import boto3
import json
import time

from src.storage.memcached import memcached_operator
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
    dataset_name = 'cifar10'
    data_bucket = "cifar10dataset"
    n_features = 32 * 32
    n_classes = 10
    host = "127.0.0.1"
    port = 11211
    tmp_bucket = "tmp-params"
    merged_bucket = "merged-params"
    cp_bucket = "cp-model"

    # training setting
    model = "mobilenet"     # mobilenet or resnet
    optim = "grad_avg"  # grad_avg or model_avg
    sync_mode = "reduce"    # async, reduce or reduce_scatter
    n_workers = 10

    # tuner configs
    tuner_strategy = "random_search"
    tuner_concurrency = 5
    lr_values = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
    lr_disc = DiscHyper("lr_discrete", lr_values)

    # hyper-parameters
    batch_size = 256
    n_epochs = 5
    start_epoch = 0
    run_epochs = 3

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
    payload['n_features'] = n_features
    payload['n_classes'] = n_classes
    payload['n_workers'] = n_workers
    payload['host'] = host
    payload['port'] = port
    payload['tmp_bucket'] = tmp_bucket
    payload['merged_bucket'] = merged_bucket
    payload['cp_bucket'] = cp_bucket
    payload['model'] = model
    payload['optim'] = optim
    payload['sync_mode'] = sync_mode
    payload['lr'] = lr
    payload['batch_size'] = batch_size
    payload['n_epochs'] = n_epochs
    payload['start_epoch'] = start_epoch
    payload['run_epochs'] = run_epochs
    payload['function_name'] = function_name

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(n_workers):
        payload['worker_index'] = i
        payload['train_file'] = 'training_{}.pt'.format(i)
        payload['test_file'] = 'test.pt'
        lambda_client.invoke(FunctionName=function_name,
                             InvocationType='Event',
                             Payload=json.dumps(payload))

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
            # lambda payload
            payload = dict()
            payload['dataset'] = dataset_name
            payload['data_bucket'] = data_bucket
            payload['n_features'] = n_features
            payload['n_classes'] = n_classes
            payload['n_workers'] = n_workers
            payload['host'] = host
            payload['port'] = port
            payload['model'] = model
            payload['optim'] = optim
            payload['sync_mode'] = sync_mode
            payload['lr'] = lr
            payload['batch_size'] = batch_size
            payload['n_epochs'] = n_epochs
            payload['start_epoch'] = start_epoch
            payload['run_epochs'] = run_epochs
            payload['function_name'] = function_name

            payload['tmp_bucket'] = tmp_bucket + "-i"
            payload['merged_bucket'] = merged_bucket + "-i"
            payload['cp_bucket'] = cp_bucket + "-i"
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
