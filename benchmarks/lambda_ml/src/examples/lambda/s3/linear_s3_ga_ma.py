import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from data_loader import libsvm_dataset

from utils.constants import Prefix, MLModel, Optimization, Synchronization
from src.storage.s3.s3_type import S3Storage
from communicator import S3Communicator

from model import linear_models


def handler(event, context):
    start_time = time.time()

    # dataset setting
    file = event['file']
    data_bucket = event['data_bucket']
    dataset_type = event['dataset_type']
    n_features = event['n_features']
    n_classes = event['n_classes']
    n_workers = event['n_workers']
    worker_index = event['worker_index']
    tmp_bucket = event['tmp_bucket']
    merged_bucket = event['merged_bucket']

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

    storage = S3Storage()
    communicator = S3Communicator(storage, tmp_bucket, merged_bucket, n_workers, worker_index)

    # Read file from s3
    read_start = time.time()
    lines = storage.load(file, data_bucket).read().decode('utf-8').split("\n")
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
        epoch_sync_time = 0
        epoch_loss = 0
        for batch_index, (items, labels) in enumerate(train_loader):
            # print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
            batch_start = time.time()
            items = Variable(items.view(-1, n_features))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            epoch_loss += loss.data
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

                    batch_sync_start = time.time()
                    postfix = "{}_{}".format(epoch, batch_index)

                    if sync_mode == "reduce":
                        w_b_grad_merge = communicator.reduce_batch(w_b_grad, postfix)
                    elif sync_mode == "reduce_scatter":
                        w_b_grad_merge = communicator.reduce_scatter_batch(w_b_grad, postfix)

                    w_grad_merge = w_b_grad_merge[:w_grad_shape[0] * w_grad_shape[1]]\
                                       .reshape(w_grad_shape) / float(n_workers)
                    b_grad_merge = w_b_grad_merge[w_grad_shape[0] * w_grad_shape[1]:]\
                                       .reshape(b_grad_shape[0]) / float(n_workers)

                    model.linear.weight.grad = Variable(torch.from_numpy(w_grad_merge))
                    model.linear.bias.grad = Variable(torch.from_numpy(b_grad_merge))
                    batch_sync_time = time.time() - batch_sync_start
                    print("one {} round cost {} s".format(sync_mode, batch_sync_time))
                    epoch_sync_time += batch_sync_time
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

                    batch_sync_start = time.time()
                    # init model
                    if worker_index == 0 and epoch == 0 and batch_index == 0:
                        storage.save(w_b.tobytes(), Prefix.w_b_prefix, merged_bucket)

                    w_b_merge = communicator.async_reduce(w_b, Prefix.w_b_prefix)
                    # do not need average
                    w_merge = w_b_merge[:w_shape[0] * w_shape[1]].reshape(w_shape)
                    b_merge = w_b_merge[w_shape[0] * w_shape[1]:].reshape(b_shape[0])
                    model.linear.weight.data = torch.from_numpy(w_merge)
                    model.linear.bias.data = torch.from_numpy(b_merge)
                    batch_sync_time = time.time() - batch_sync_start
                    print("one {} round cost {} s".format(sync_mode, batch_sync_time))
                    epoch_sync_time += batch_sync_time

            if sync_mode != "async":
                step_start = time.time()
                optimizer.step()
                epoch_cal_time += time.time() - step_start

            # print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f s, Loss: %.4f, batch cost %.4f s'
            #        % (epoch + 1, n_epochs, batch_index + 1, n_train_batch,
            #           time.time() - train_start, loss.data, time.time() - batch_start))

        if optim == "model_avg":
            w = model.linear.weight.data.numpy()
            w_shape = w.shape
            b = model.linear.bias.data.numpy()
            b_shape = b.shape
            w_b = np.concatenate((w.flatten(), b.flatten()))
            epoch_cal_time += time.time() - epoch_start

            epoch_sync_start = time.time()
            postfix = str(epoch)

            if sync_mode == "reduce":
                w_b_merge = communicator.reduce_epoch(w_b, postfix)
            elif sync_mode == "reduce_scatter":
                w_b_merge = communicator.reduce_scatter_epoch(w_b, postfix)
            elif sync_mode == "async":
                if epoch == 0:
                    storage.save(w_b.tobytes(), Prefix.w_b_prefix, merged_bucket)
                w_b_merge = communicator.async_reduce(w_b, Prefix.w_b_prefix)

            w_merge = w_b_merge[:w_shape[0] * w_shape[1]].reshape(w_shape)
            b_merge = w_b_merge[w_shape[0] * w_shape[1]:].reshape(b_shape[0])
            if sync_mode == "reduce" or sync_mode == "reduce_scatter":
                w_merge = w_merge / float(n_workers)
                b_merge = b_merge / float(n_workers)
            model.linear.weight.data = torch.from_numpy(w_merge)
            model.linear.bias.data = torch.from_numpy(b_merge)
            print("one {} round cost {} s".format(sync_mode, time.time() - epoch_sync_start))
            epoch_sync_time += time.time() - epoch_sync_start

        if worker_index == 0:
            delete_start = time.time()
            # model avg delete by epoch
            if optim == "model_avg" and sync_mode != "async":
                communicator.delete_expired_epoch(epoch)
            elif optim == "grad_avg" and sync_mode != "async":
                communicator.delete_expired_batch(epoch, batch_index)
            epoch_sync_time += time.time() - delete_start

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
              'calculation cost = %.4f s, synchronization cost %.4f s, test cost %.4f s, '
              'accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (epoch + 1, n_epochs, batch_index + 1, n_train_batch,
                 time.time() - train_start, epoch_loss.data, time.time() - epoch_start,
                 epoch_cal_time, epoch_sync_time, test_time,
                 n_test, 100. * n_test_correct / n_test, test_loss / n_test))

    if worker_index == 0:
        storage.clear(tmp_bucket)
        storage.clear(merged_bucket)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
