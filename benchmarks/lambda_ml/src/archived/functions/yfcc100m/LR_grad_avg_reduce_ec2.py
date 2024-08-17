import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.s3.get_object import get_object
from archived.elasticache.Memcached import memcached_init
from archived.sync import reduce_batch, clear_bucket

from archived.old_model import LogisticRegression
from data_loader.YFCCLibsvmDataset import DenseLibsvmDataset

# lambda setting
local_dir = "/tmp"

# algorithm setting
validation_ratio = .1
shuffle_dataset = True
random_seed = 42


def handler(event, context):
    start_time = time.time()
    bucket = event['bucket_name']
    worker_index = event['rank']
    num_workers = event['num_workers']
    key = event['file']
    merged_bucket = event['merged_bucket']
    num_classes = event['num_classes']
    num_features = event['num_features']
    pos_tag = event['pos_tag']
    num_epochs = event['num_epochs']
    learning_rate = event['learning_rate']
    batch_size = event['batch_size']
    elasti_location = event['elasticache']
    endpoint = memcached_init(elasti_location)

    print('bucket = {}'.format(bucket))
    print("file = {}".format(key))
    print('merged bucket = {}'.format(merged_bucket))
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('num epochs = {}'.format(num_epochs))
    print('learning rate = {}'.format(learning_rate))
    print("batch size = {}".format(batch_size))

    # read file from s3
    file = get_object(bucket, key).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - start_time))

    parse_start = time.time()
    dataset = DenseLibsvmDataset(file, num_features, pos_tag)

    totol_count = dataset.__len__()
    pos_count = 0
    for i in range(totol_count):
        if dataset.__getitem__(i)[1] == 1:
            pos_count += 1
    print("{} positive observations out of {}".format(pos_count, totol_count))

    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()
    # Creating data indices for training and validation splits:
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    if shuffle_dataset:
        np.random.seed(random_seed)
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating PT data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)

    train_loader = torch.utils.data.DataLoader(dataset,
                                               batch_size=batch_size,
                                               sampler=train_sampler)
    validation_loader = torch.utils.data.DataLoader(dataset,
                                                    batch_size=batch_size,
                                                    sampler=valid_sampler)

    print("preprocess data cost {} s".format(time.time() - preprocess_start))

    model = LogisticRegression(num_features, num_classes)

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # Training the Model
    train_start = time.time()
    for epoch in range(num_epochs):
        epoch_start = time.time()
        epoch_loss = 0
        cal_time = 0
        sync_time = 0
        for batch_index, (items, labels) in enumerate(train_loader):
            batch_start = time.time()
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            epoch_loss += loss.data
            loss.backward()

            w_grad = model.linear.weight.grad.data.numpy()
            w_grad_shape = w_grad.shape
            b_grad = model.linear.bias.grad.data.numpy()
            b_grad_shape = b_grad.shape

            w_b_grad = np.concatenate((w_grad.flatten(), b_grad.flatten()))
            cal_time += time.time() - batch_start

            sync_start = time.time()
            postfix = "{}_{}".format(epoch, batch_index)
            w_b_grad_merge = reduce_batch(endpoint, w_b_grad, merged_bucket,
                                          num_workers, worker_index, postfix)
            w_grad_merge = \
                w_b_grad_merge[:w_grad_shape[0] * w_grad_shape[1]].reshape(w_grad_shape) / float(num_workers)
            b_grad_merge = \
                w_b_grad_merge[w_grad_shape[0] * w_grad_shape[1]:].reshape(b_grad_shape[0]) / float(num_workers)

            model.linear.weight.grad = Variable(torch.from_numpy(w_grad_merge))
            model.linear.bias.grad = Variable(torch.from_numpy(b_grad_merge))
            sync_time += time.time() - sync_start

            optimizer.step()

            # print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f, Loss: %.4f, epoch cost %.4f, '
            #       'batch cost %.4f s: cal cost %.4f s communication cost %.4f s, '
            #       % (epoch + 1, num_epochs, batch_index, len(train_indices) / batch_size,
            #          time.time() - train_start, loss.data, time.time() - epoch_start,
            #          time.time() - batch_start, cal_time, sync_time))

        # Test the Model
        test_start = time.time()
        correct = 0
        total = 0
        test_loss = 0
        for items, labels in validation_loader:
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)
            outputs = model(items)
            test_loss += criterion(outputs, labels).data
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()
        test_time = time.time() - test_start

        print('Epoch %d has %d batches, time = %.4f, epoch cost %.4f s: '
              'computation cost %.4f s communication cost %.4f s, '
              'train loss = %.4f, test cost %.4f s, accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (epoch, batch_index, time.time() - train_start, time.time() - epoch_start, cal_time, sync_time,
                 epoch_loss, test_time, len(val_indices), 100 * correct / total, test_loss / total))

    if worker_index == 0:
        clear_bucket(endpoint)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
