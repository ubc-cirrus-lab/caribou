from dataset_utils import preprocess_cifar10
import os
import boto3
import torch
import json
import time

def handler(event,context):
    dataset_name = 'cifar10'
    bucket_name = 'cifar10dataset'
    key = 'cifar-10-python.tar.gz'
    num_workers = 10
    data_path = os.path.join(os.sep, 'tmp', 'data')
	
    # preprocess_start = time.time()
    # preprocess_cifar10(bucket_name = bucket_name, key = key, data_path = data_path, num_workers = num_workers)
    # print("pre-process mnist cost {} s".format(time.time()-preprocess_start))
	
	
	# invoke functions
    payload = dict()
    payload['dataset'] = dataset_name
    payload['data_bucket'] = bucket_name
	# payload['model_bucket'] = 'model_bucket'
    payload['num_workers'] = num_workers
	
	# invoke functions
    lambda_client = boto3.client('lambda')
    for i in range(num_workers):
        payload['rank'] = i
        payload['keys_training_data'] = 'training_{}.pt'.format(i)
        lambda_client.invoke(FunctionName='cifar10_F2', InvocationType='Event', Payload=json.dumps(payload))

