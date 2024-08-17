import time
import numpy as np
import pickle

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.elasticache.Memcached import hset_object
from archived.elasticache.Memcached import memcached_init
from archived.s3.get_object import get_object
from archived.s3 import put_object
from archived.sync import merge_w_b_grads, put_merged_w_b_grads, get_merged_w_b_grads


from archived.old_model.SVM import SVM
from data_loader.libsvm_dataset import DenseDatasetWithLines

# lambda setting
local_dir = "/tmp"
w_prefix = "w_"
b_prefix = "b_"

# algorithm setting
num_features = 30
num_classes = 2
learning_rate = 0.01
batch_size = 1000
num_epochs = 2
validation_ratio = .2
shuffle_dataset = True
random_seed = 42


def handler(event, context):
    start_time = time.time()
    bucket = event['bucket_name']
    worker_index = event['rank']
    num_workers = event['num_workers']
    key = event['file']
    merged_bucket = event['merged_bucket']
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
    dataset = DenseDatasetWithLines(file, num_features)
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

    model = SVM(num_features, num_classes)
    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    train_loss = []
    test_loss = []
    test_acc = []
    epoch_time = 0
    # Training the Model
    epoch_start = time.time()
    for epoch in range(num_epochs):
        tmp_train = 0
        for batch_index, (items, labels) in enumerate(train_loader):
            print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
            batch_start = time.time()
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            if (batch_index + 1) % 1 == 0:
                print('Epoch: [%d/%d], Step: [%d/%d], Loss: %.4f'
                      % (epoch + 1, num_epochs, batch_index + 1, len(train_indices) / batch_size, loss.data))
            tmp_train = tmp_train + loss.item()

        train_loss.append(tmp_train / (batch_index + 1))
        # sync model
        w_model = model.linear.weight.data.numpy()
        b_model = model.linear.bias.data.numpy()
        epoch_time = time.time() - epoch_start + epoch_time
        # synchronization starts from that every worker writes their model after this epoch
        sync_start = time.time()
        hset_object(endpoint, merged_bucket, w_prefix + str(worker_index), w_model.tobytes())
        hset_object(endpoint, merged_bucket, b_prefix + str(worker_index), b_model.tobytes())
        tmp_write_local_epoch_time = time.time() - sync_start
        print("write local model cost = {}".format(tmp_write_local_epoch_time))

        # merge gradients among files
        file_postfix = "{}".format(epoch)
        if worker_index == 0:
            merge_start = time.time()
            w_model_merge, b_model_merge = merge_w_b_grads(endpoint,
                                                           merged_bucket, num_workers, w_model.dtype,
                                                           w_model.shape, b_model.shape,
                                                           w_prefix, b_prefix)
            put_merged_w_b_grads(endpoint, merged_bucket,
                                 w_model_merge, b_model_merge, file_postfix,
                                 w_prefix, b_prefix)
        else:
            w_model_merge, b_model_merge = get_merged_w_b_grads(endpoint, merged_bucket, file_postfix,
                                                                w_model.dtype, w_model.shape, b_model.shape,
                                                                w_prefix, b_prefix)

        model.linear.weight.data = Variable(torch.from_numpy(w_model_merge))
        model.linear.bias.data = Variable(torch.from_numpy(b_model_merge))

        tmp_sync_time = time.time() - sync_start
        print("synchronization cost {} s".format(tmp_sync_time))

        # Test the Model
        correct = 0
        total = 0
        count = 0
        tmp_test = 0
        for items, labels in validation_loader:
            items = Variable(items.view(-1, num_features))
            outputs = model(items)
            loss = criterion(outputs, labels)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()
            tmp_test = tmp_test + loss.item()
            count = count + 1
        # print('Accuracy of the model on the %d test samples: %d %%' % (len(val_indices), 100 * correct / total))
        test_acc.append(100 * correct / total)
        test_loss.append(tmp_test / count)
        epoch_start = time.time()
    end_time = time.time()

    print("elapsed time = {} s".format(end_time - start_time))
    loss_record = [test_loss, test_acc, train_loss, epoch_time]
    put_object("model-average-loss", "average_loss{}".format(worker_index), pickle.dumps(loss_record))
