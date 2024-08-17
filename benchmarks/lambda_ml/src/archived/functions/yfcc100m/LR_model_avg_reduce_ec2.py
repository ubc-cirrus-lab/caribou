import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.s3.get_object import get_object
from archived.elasticache.Memcached import memcached_init
from archived.sync import reduce_epoch, clear_bucket

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
    key = event['file'].split(",")
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
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('merge bucket = {}'.format(merged_bucket))
    print('num epochs = {}'.format(num_epochs))
    print('num classes = {}'.format(num_classes))
    print('num features = {}'.format(num_features))
    print('positive tag = {}'.format(pos_tag))
    print('learning rate = {}'.format(learning_rate))
    print("batch_size = {}".format(batch_size))

    # read file from s3
    file = get_object(bucket, key[0]).read().decode('utf-8').split("\n")
    dataset = DenseLibsvmDataset(file, num_features, pos_tag)
    if len(key) > 1:
        for more_key in key[1:]:
            file = get_object(bucket, more_key).read().decode('utf-8').split("\n")
            dataset.add_more(file)
    print("read data cost {} s".format(time.time() - start_time))

    parse_start = time.time()
    total_count = dataset.__len__()
    pos_count = 0
    for i in range(total_count):
        if dataset.__getitem__(i)[1] == 1:
            pos_count += 1
    print("{} positive observations out of {}".format(pos_count, total_count))
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

    print("preprocess data cost {} s, dataset size = {}"
          .format(time.time() - preprocess_start, dataset_size))

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

            optimizer.step()

        w = model.linear.weight.data.numpy()
        w_shape = w.shape
        b = model.linear.bias.data.numpy()
        b_shape = b.shape
        w_and_b = np.concatenate((w.flatten(), b.flatten()))
        cal_time = time.time() - epoch_start

        sync_start = time.time()
        postfix = str(epoch)
        u_w_b_merge = reduce_epoch(endpoint, w_and_b, merged_bucket,
                                   num_workers, worker_index, postfix)

        w_mean = u_w_b_merge[: w_shape[0] * w_shape[1]].reshape(w_shape) / float(num_workers)
        b_mean = u_w_b_merge[w_shape[0] * w_shape[1]:].reshape(b_shape[0]) / float(num_workers)
        model.linear.weight.data = torch.from_numpy(w_mean)
        model.linear.bias.data = torch.from_numpy(b_mean)
        sync_time = time.time() - sync_start

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

        print('Epoch: [%d/%d] has %d batches, Time: %.4f, Loss: %.4f, '
              'epoch cost %.4f: computation cost %.4f s communication cost %.4f s test cost %.4f s, '
              'accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (epoch + 1, num_epochs, batch_index, time.time() - train_start, epoch_loss.data,
                 time.time() - epoch_start, cal_time, sync_time, test_time,
                 len(val_indices), 100 * correct / total, test_loss / total))

    # Test the Model
    correct = 0
    total = 0
    for items, labels in validation_loader:
        items = Variable(items.view(-1, num_features))
        # items = Variable(items)
        outputs = model(items)
        _, predicted = torch.max(outputs.data, 1)
        total += labels.size(0)
        correct += (predicted == labels).sum()

    print('Accuracy of the model on the %d test samples: %d %%' % (len(val_indices), 100 * correct / total))

    if worker_index == 0:
        clear_bucket(endpoint)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
