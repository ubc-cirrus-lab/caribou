import concurrent.futures

from caribou.deployment.client import CaribouWorkflow
import json
import boto3

import time
import numpy as np
from typing import Any

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from src.storage.dynamo.dynamo_type import DynamoTable
from src.storage.dynamo import dynamo_operator
from src.storage.s3.s3_type import S3Storage
from src.communicator import DynamoCommunicator
from src.model import linear_models
from src.data_loader import libsvm_dataset
from src.utils.constants import Prefix, MLModel, Optimization, Synchronization

workflow = CaribouWorkflow(name="lambda_ml", version="0.0.6")


@workflow.serverless_function(name="LR_Trigger", entry_point=True)
def trigger(event: dict[str, Any]) -> dict[str, Any]:
    function_name = "LR_higgs"

    # dataset setting
    dataset_name = 'higgs'
    data_bucket = "higgs-10"
    dataset_type = "dense_libsvm"  # dense_libsvm
    n_features = 30
    n_classes = 2
    tmp_table_name = "lambda-ml-tmp-params"
    merged_table_name = "lambda-ml-merged-params"
    key_col = "key"

    # training setting
    model = "lr"  # lr, svm
    optim = "grad_avg"  # grad_avg, model_avg, or admm
    sync_mode = "reduce"  # async, reduce or reduce_scatter
    n_workers = 4

    # hyper-parameters
    lr = 0.01
    batch_size = 100000
    n_epochs = 2
    valid_ratio = .2
    n_admm_epochs = 2
    lam = 0.01
    rho = 0.01

    # clear dynamodb table
    dynamo_client = dynamo_operator.get_client()
    tmp_tb = DynamoTable(dynamo_client, tmp_table_name)
    merged_tb = DynamoTable(dynamo_client, tmp_table_name)
    tmp_tb.clear(key_col)
    merged_tb.clear(key_col)

    # lambda payload
    payload = {'dataset': dataset_name, 'data_bucket': data_bucket, 'dataset_type': dataset_type,
               'n_features': n_features, 'n_classes': n_classes, 'n_workers': n_workers,
               'tmp_table_name': tmp_table_name, 'merged_table_name': merged_table_name, 'key_col': key_col,
               'model': model, 'optim': optim, 'sync_mode': sync_mode, 'lr': lr, 'batch_size': batch_size,
               'n_epochs': n_epochs, 'valid_ratio': valid_ratio, 'n_admm_epochs': n_admm_epochs, 'lambda': lam,
               'rho': rho, 'worker_index': 0, 'file': '{}_{}'.format(0, n_workers)}

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def worker0():
        copy_payload = dict(payload)
        copy_payload["worker_index"] = 0
        copy_payload['file'] = '{}_{}'.format(0, n_workers)
        workflow.invoke_serverless_function(worker, copy_payload)

    def worker1():
        copy_payload = dict(payload)
        copy_payload["worker_index"] = 1
        copy_payload['file'] = '{}_{}'.format(1, n_workers)
        workflow.invoke_serverless_function(worker, copy_payload)

    def worker2():
        copy_payload = dict(payload)
        copy_payload["worker_index"] = 2
        copy_payload['file'] = '{}_{}'.format(2, n_workers)
        workflow.invoke_serverless_function(worker, copy_payload)

    def worker3():
        copy_payload = dict(payload)
        copy_payload["worker_index"] = 3
        copy_payload['file'] = '{}_{}'.format(3, n_workers)
        workflow.invoke_serverless_function(worker, copy_payload)

    pool.submit(worker0)
    pool.submit(worker1)
    pool.submit(worker2)
    pool.submit(worker3)

    pool.shutdown(wait=True)

    return {"status": 200}


@workflow.serverless_function(name="LR_Worker", entry_point=False)
def worker(event: dict[str, Any]) -> dict[str, Any]:
    start_time = time.time()

    # dataset setting
    file = event['file']
    data_bucket = event['data_bucket']
    dataset_type = event['dataset_type']
    assert dataset_type == "dense_libsvm"
    n_features = event['n_features']
    n_classes = event['n_classes']
    n_workers = event['n_workers']
    worker_index = event['worker_index']
    tmp_table_name = event['tmp_table_name']
    merged_table_name = event['merged_table_name']
    key_col = event['key_col']

    # training setting
    model_name = event['model']
    optim = event['optim']
    sync_mode = event['sync_mode']
    assert model_name.lower() in MLModel.Linear_Models
    assert optim.lower() in Optimization.All
    assert sync_mode.lower() in Synchronization.All

    # hyper-parameter
    learning_rate = event['lr']
    batch_size = event['batch_size']
    n_epochs = event['n_epochs']
    valid_ratio = event['valid_ratio']

    shuffle_dataset = True
    random_seed = 100

    print('bucket = {}'.format(data_bucket))
    print("file = {}".format(file))
    print('number of workers = {}'.format(n_workers))
    print('worker index = {}'.format(worker_index))
    print('model = {}'.format(model_name))
    print('optimization = {}'.format(optim))
    print('sync mode = {}'.format(sync_mode))

    s3_storage = S3Storage()
    dynamo_client = dynamo_operator.get_client()
    tmp_table = DynamoTable(dynamo_client, tmp_table_name)
    merged_table = DynamoTable(dynamo_client, merged_table_name)
    communicator = DynamoCommunicator(dynamo_client, tmp_table, merged_table, key_col, n_workers, worker_index)

    # Read file from s3
    read_start = time.time()
    lines = s3_storage.load(file, data_bucket).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - read_start))

    parse_start = time.time()
    dataset = libsvm_dataset.from_lines(lines, n_features, dataset_type)
    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()
    # Creating data indices for training and validation splits:
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(valid_ratio * dataset_size))
    if shuffle_dataset:
        np.random.seed(random_seed)
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)
    train_loader = torch.utils.data.DataLoader(dataset,
                                               batch_size=batch_size,
                                               sampler=train_sampler)
    n_train_batch = len(train_loader)
    validation_loader = torch.utils.data.DataLoader(dataset,
                                                    batch_size=batch_size,
                                                    sampler=valid_sampler)
    print("preprocess data cost {} s, dataset size = {}"
          .format(time.time() - preprocess_start, dataset_size))

    model = linear_models.get_model(model_name, n_features, n_classes)

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    train_start = time.time()
    # Training the Model
    for epoch in range(n_epochs):
        epoch_start = time.time()
        epoch_cal_time = 0
        epoch_comm_time = 0
        epoch_loss = 0
        for batch_idx, (items, labels) in enumerate(train_loader):
            # print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
            batch_start = time.time()
            items = Variable(items.view(-1, n_features))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            epoch_loss += loss.item()
            loss.backward()

            if optim == "grad_avg":
                if sync_mode == "reduce" or sync_mode == "reduce_scatter":
                    w_grad = model.linear.weight.grad.data.numpy()
                    w_grad_shape = w_grad.shape
                    b_grad = model.linear.bias.grad.data.numpy()
                    b_grad_shape = b_grad.shape
                    w_b_grad = np.concatenate((w_grad.flatten(), b_grad.flatten()))
                    batch_cal_time = time.time() - batch_start
                    epoch_cal_time += batch_cal_time

                    batch_comm_start = time.time()

                    if sync_mode == "reduce":
                        w_b_grad_merge = communicator.reduce_batch(w_b_grad, epoch, batch_idx)
                    elif sync_mode == "reduce_scatter":
                        w_b_grad_merge = communicator.reduce_scatter_batch(w_b_grad, epoch, batch_idx)

                    w_grad_merge = w_b_grad_merge[:w_grad_shape[0] * w_grad_shape[1]] \
                                       .reshape(w_grad_shape) / float(n_workers)
                    b_grad_merge = w_b_grad_merge[w_grad_shape[0] * w_grad_shape[1]:] \
                                       .reshape(b_grad_shape[0]) / float(n_workers)

                    model.linear.weight.grad = Variable(torch.from_numpy(w_grad_merge))
                    model.linear.bias.grad = Variable(torch.from_numpy(b_grad_merge))
                    batch_comm_time = time.time() - batch_comm_start
                    print("one {} round cost {} s".format(sync_mode, batch_comm_time))
                    epoch_comm_time += batch_comm_time
                elif sync_mode == "async":
                    # async does step before sync
                    optimizer.step()
                    w = model.linear.weight.data.numpy()
                    w_shape = w.shape
                    b = model.linear.bias.data.numpy()
                    b_shape = b.shape
                    w_b = np.concatenate((w.flatten(), b.flatten()))
                    batch_cal_time = time.time() - epoch_start
                    epoch_cal_time += batch_cal_time

                    batch_comm_start = time.time()
                    # init model
                    if worker_index == 0 and epoch == 0 and batch_idx == 0:
                        merged_table.save(w_b.tobytes(), Prefix.w_b_prefix, key_col)

                    w_b_merge = communicator.async_reduce(w_b, Prefix.w_b_prefix)
                    # do not need average
                    w_merge = w_b_merge[:w_shape[0] * w_shape[1]].reshape(w_shape)
                    b_merge = w_b_merge[w_shape[0] * w_shape[1]:].reshape(b_shape[0])
                    model.linear.weight.data = torch.from_numpy(w_merge)
                    model.linear.bias.data = torch.from_numpy(b_merge)
                    batch_comm_time = time.time() - batch_comm_start
                    print("one {} round cost {} s".format(sync_mode, batch_comm_time))
                    epoch_comm_time += batch_comm_time

            if sync_mode != "async":
                step_start = time.time()
                optimizer.step()
                epoch_cal_time += time.time() - step_start

            if batch_idx % 10 == 0:
                print("Epoch: [%d/%d], Step: [%d/%d], Time: %.4f s, Loss: %.4f, batch cost %.4f s"
                      % (epoch + 1, n_epochs, batch_idx + 1, n_train_batch,
                         time.time() - train_start, loss.item(), time.time() - batch_start))

        if optim == "model_avg":
            w = model.linear.weight.data.numpy()
            w_shape = w.shape
            b = model.linear.bias.data.numpy()
            b_shape = b.shape
            w_b = np.concatenate((w.flatten(), b.flatten()))
            epoch_cal_time += time.time() - epoch_start

            epoch_comm_start = time.time()

            if sync_mode == "reduce":
                w_b_merge = communicator.reduce_epoch(w_b, epoch)
            elif sync_mode == "reduce_scatter":
                w_b_merge = communicator.reduce_scatter_epoch(w_b, epoch)
            elif sync_mode == "async":
                if worker_index == 0 and epoch == 0:
                    merged_table.save(w_b.tobytes(), Prefix.w_b_prefix, key_col)
                w_b_merge = communicator.async_reduce(w_b, Prefix.w_b_prefix)

            w_merge = w_b_merge[:w_shape[0] * w_shape[1]].reshape(w_shape)
            b_merge = w_b_merge[w_shape[0] * w_shape[1]:].reshape(b_shape[0])
            if sync_mode == "reduce" or sync_mode == "reduce_scatter":
                w_merge = w_merge / float(n_workers)
                b_merge = b_merge / float(n_workers)
            model.linear.weight.data = torch.from_numpy(w_merge)
            model.linear.bias.data = torch.from_numpy(b_merge)
            print("one {} round cost {} s".format(sync_mode, time.time() - epoch_comm_start))
            epoch_comm_time += time.time() - epoch_comm_start

        if worker_index == 0:
            delete_start = time.time()
            # model avg delete by epoch
            if optim == "model_avg" and sync_mode != "async":
                communicator.delete_expired_epoch(epoch)
            elif optim == "grad_avg" and sync_mode != "async":
                communicator.delete_expired_batch(epoch, batch_idx)
            epoch_comm_time += time.time() - delete_start

        # Test the Model
        test_start = time.time()
        n_test_correct = 0
        n_test = 0
        test_loss = 0
        for items, labels in validation_loader:
            items = Variable(items.view(-1, n_features))
            labels = Variable(labels)
            outputs = model(items)
            test_loss += criterion(outputs, labels).data
            _, predicted = torch.max(outputs.data, 1)
            n_test += labels.size(0)
            n_test_correct += (predicted == labels).sum()
        test_time = time.time() - test_start

        print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f, Loss: %.4f, epoch cost %.4f: '
              'calculation cost = %.4f s, communication cost %.4f s, test cost %.4f s, '
              'accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (epoch + 1, n_epochs, batch_idx + 1, n_train_batch,
                 time.time() - train_start, epoch_loss, time.time() - epoch_start,
                 epoch_cal_time, epoch_comm_time, test_time,
                 n_test, 100. * n_test_correct / n_test, test_loss / n_test))

    if worker_index == 0:
        tmp_table.clear(key_col)
        merged_table.clear(key_col)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))

    return {"status": 200}
