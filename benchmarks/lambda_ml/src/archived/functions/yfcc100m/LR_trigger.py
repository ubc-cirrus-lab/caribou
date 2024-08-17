import boto3
import json

from archived.sync import clear_bucket


def handler(event, context):
    dataset_name = 'yfcc100m'
    bucket_name = "yfcc100m-400"
    num_workers = 10
    tmp_bucket = "tmp-params"
    merged_bucket = "merged-params"
    num_epochs = 10
    num_admm_epochs = 10
    learning_rate = 0.1
    batch_size = 500
    lam = 0.01
    rho = 0.01
    num_classes = 2
    num_features = 4096
    pos_tag = "animal"
    files_per_worker = 4

    clear_bucket(tmp_bucket)
    clear_bucket(merged_bucket)

    # invoke functions
    payload = dict()
    payload['dataset'] = dataset_name
    payload['bucket_name'] = bucket_name
    payload['num_workers'] = num_workers
    payload['tmp_bucket'] = tmp_bucket
    payload['merged_bucket'] = merged_bucket
    payload['num_epochs'] = num_epochs
    payload['num_admm_epochs'] = num_admm_epochs
    payload['learning_rate'] = learning_rate
    payload['batch_size'] = batch_size
    payload['lambda'] = lam
    payload['rho'] = rho
    payload['num_classes'] = num_classes
    payload['num_features'] = num_features
    payload['pos_tag'] = pos_tag

    # invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(num_workers):
        payload['rank'] = i
        start_file_index = i * files_per_worker
        payload['file'] = ",".join(range(start_file_index + files_per_worker))
        lambda_client.invoke(FunctionName='LR_higgs', InvocationType='Event', Payload=json.dumps(payload))
