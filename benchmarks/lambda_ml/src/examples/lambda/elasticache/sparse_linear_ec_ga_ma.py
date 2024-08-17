import time
import math

import numpy as np

import torch

from data_loader import libsvm_dataset

from utils.constants import Prefix, MLModel, Optimization, Synchronization
from src.storage import S3Storage, MemcachedStorage
from communicator import MemcachedCommunicator

from model import linear_models


def handler(event, context):
    start_time = time.time()

    # dataset setting
    file = event['file']
    data_bucket = event['data_bucket']
    dataset_type = event['dataset_type']
    assert dataset_type == "sparse_libsvm"
    n_features = event['n_features']
    n_classes = event['n_classes']
    n_workers = event['n_workers']
    worker_index = event['worker_index']
    host = event['host']
    port = event['port']
    tmp_bucket = event['tmp_bucket']
    merged_bucket = event['merged_bucket']

    # training setting
    model_name = event['model']
    optim = event['optim']
    sync_mode = event['sync_mode']
    assert model_name.lower() in MLModel.Sparse_Linear_Models
    assert optim.lower() in [Optimization.Grad_Avg, Optimization.Model_Avg]
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
    mem_storage = MemcachedStorage(host, port)
    communicator = MemcachedCommunicator(mem_storage, tmp_bucket, merged_bucket, n_workers, worker_index)
    if worker_index == 0:
        mem_storage.clear()

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

    # split train set and test set
    train_set = [dataset[i] for i in train_indices]
    n_train_batch = math.floor(len(train_set) / batch_size)
    val_set = [dataset[i] for i in val_indices]
    print("preprocess data cost {} s, dataset size = {}"
          .format(time.time() - preprocess_start, dataset_size))

    model = linear_models.get_sparse_model(model_name, train_set, val_set, n_features,
                                           n_epochs, learning_rate, batch_size)

    train_start = time.time()
    # Training the Model
    for epoch in range(n_epochs):
        epoch_start = time.time()
        epoch_cal_time = 0
        epoch_comm_time = 0
        epoch_loss = 0.

        for batch_idx in range(n_train_batch):
            batch_start = time.time()
            batch_loss, batch_acc = model.one_batch()
            epoch_loss += batch_loss.average

            if optim == "grad_avg":
                if sync_mode == "reduce" or sync_mode == "reduce_scatter":
                    w_b = np.concatenate((model.weight.numpy().flatten(), np.array([model.bias], dtype=np.float32)))
                    batch_cal_time = time.time() - batch_start
                    epoch_cal_time += batch_cal_time
                    print("batch cal cost {} s".format(batch_cal_time))

                    batch_comm_start = time.time()

                    if sync_mode == "reduce":
                        w_b_merge = communicator.reduce_batch(w_b, epoch, batch_idx)
                    elif sync_mode == "reduce_scatter":
                        w_b_merge = communicator.reduce_scatter_batch(w_b, epoch, batch_idx)

                    w_merge = w_b_merge[:n_features] / float(n_workers)
                    b_merge = w_b_merge[-1] / float(n_workers)
                    model.weight = torch.from_numpy(w_merge).reshape(n_features, 1)
                    model.bias = float(b_merge)

                    batch_comm_time = time.time() - batch_comm_start
                    print("one {} round cost {} s".format(sync_mode, batch_comm_time))
                    epoch_comm_time += batch_comm_time
                elif sync_mode == "async":
                    w_b = np.concatenate((model.weight.numpy().flatten(), np.array([model.bias], dtype=np.float32)))
                    batch_cal_time = time.time() - batch_start
                    epoch_cal_time += batch_cal_time

                    batch_comm_start = time.time()
                    # init model
                    if worker_index == 0 and epoch == 0 and batch_idx == 0:
                        mem_storage.save_v2(w_b.tobytes(), Prefix.w_b_prefix, merged_bucket)

                    w_b_merge = communicator.async_reduce(w_b, Prefix.w_b_prefix)
                    # async des not need average
                    w_merge = w_b_merge[:n_features]
                    b_merge = w_b_merge[-1]
                    model.weight = torch.from_numpy(w_merge).reshape(n_features, 1)
                    model.bias = float(b_merge)

                    batch_comm_time = time.time() - batch_comm_start
                    print("one {} round cost {} s".format(sync_mode, batch_comm_time))
                    epoch_comm_time += batch_comm_time

            if batch_idx % 10 == 0:
                print('Epoch: [%d/%d], Batch: [%d/%d], Time: %.4f s, Loss: %.4f, Accuracy: %.4f, batch cost %.4f s'
                      % (epoch + 1, n_epochs, batch_idx + 1, n_train_batch, time.time() - train_start,
                         batch_loss.average, batch_acc.accuracy, time.time() - batch_start))

        if optim == "model_avg":
            w_b = np.concatenate((model.weight.numpy().flatten(), np.array([model.bias], dtype=np.float32)))
            epoch_cal_time += time.time() - epoch_start

            epoch_sync_start = time.time()

            if sync_mode == "reduce":
                w_b_merge = communicator.reduce_epoch(w_b, epoch)
            elif sync_mode == "reduce_scatter":
                w_b_merge = communicator.reduce_scatter_epoch(w_b, epoch)
            elif sync_mode == "async":
                if worker_index == 0 and epoch == 0:
                    mem_storage.save_v2(w_b.tobytes(), Prefix.w_b_prefix, merged_bucket)
                w_b_merge = communicator.async_reduce(w_b, Prefix.w_b_prefix)

            w_merge = w_b_merge[:n_features]
            b_merge = w_b_merge[-1]
            # async des not need average
            if sync_mode == "reduce" or sync_mode == "reduce_scatter":
                w_merge = w_merge / float(n_workers)
                b_merge = b_merge / float(n_workers)
            model.weight = torch.from_numpy(w_merge).reshape(n_features, 1)
            model.bias = float(b_merge)
            print("one {} round cost {} s".format(sync_mode, time.time() - epoch_sync_start))
            epoch_comm_time += time.time() - epoch_sync_start

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
        test_loss, test_acc = model.evaluate()
        test_time = time.time() - test_start

        print("Epoch: [{}/{}] finishes, Batch: [{}/{}], Time: {:.4f}, Loss: {:.4f}, epoch cost {:.4f} s, "
              "calculation cost = {:.4f} s, synchronization cost {:.4f} s, test cost {:.4f} s, "
              "accuracy of the model on the {} test samples: {}, loss = {}"
              .format(epoch + 1, n_epochs, batch_idx + 1, n_train_batch,
                      time.time() - train_start, epoch_loss, time.time() - epoch_start,
                      epoch_cal_time, epoch_comm_time, test_time,
                      len(val_set), test_acc.accuracy, test_loss.average))

    if worker_index == 0:
        mem_storage.clear()

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
