import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.nn import Parameter
from torch.utils.data.sampler import SubsetRandomSampler

from data_loader import libsvm_dataset

from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from utils.constants import Prefix, MLModel, Optimization, Synchronization
from src.storage.s3.s3_type import S3Storage

from model import linear_models


def handler(event, context):
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

    # ps setting
    host = event['host']
    port = event['port']

    # training setting
    model_name = event['model']
    optim = event['optim']
    sync_mode = event['sync_mode']
    assert model_name.lower() in MLModel.Linear_Models
    assert optim.lower() == Optimization.Grad_Avg
    assert sync_mode.lower() == Synchronization.Reduce

    # hyper-parameter
    learning_rate = event['lr']
    batch_size = event['batch_size']
    n_epochs = event['n_epochs']
    valid_ratio = event['valid_ratio']

    print('bucket = {}'.format(data_bucket))
    print("file = {}".format(file))
    print('number of workers = {}'.format(n_workers))
    print('worker index = {}'.format(worker_index))
    print('model = {}'.format(model_name))
    print('host = {}'.format(host))
    print('port = {}'.format(port))

    # Set thrift connection
    # Make socket
    transport = TSocket.TSocket(host, port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    t_client = ParameterServer.Client(protocol)
    # Connect!
    transport.open()
    # test thrift connection
    ps_client.ping(t_client)
    print("create and ping thrift server >>> HOST = {}, PORT = {}".format(host, port))

    # Read file from s3
    read_start = time.time()
    storage = S3Storage()
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

    shuffle_dataset = True
    random_seed = 100
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

    # register model
    model_name = "w.b"
    weight_shape = model.linear.weight.data.numpy().shape
    weight_length = weight_shape[0] * weight_shape[1]
    bias_shape = model.linear.bias.data.numpy().shape
    bias_length = bias_shape[0]
    model_length = weight_length + bias_length
    ps_client.register_model(t_client, worker_index, model_name, model_length, n_workers)
    ps_client.exist_model(t_client, model_name)
    print("register and check model >>> name = {}, length = {}".format(model_name, model_length))

    # Training the Model
    train_start = time.time()
    iter_counter = 0
    for epoch in range(n_epochs):
        epoch_start = time.time()
        epoch_cal_time = 0
        epoch_comm_time = 0
        epoch_loss = 0.
        for batch_idx, (items, labels) in enumerate(train_loader):
            batch_comm_time = 0

            batch_start = time.time()
            # pull latest model
            ps_client.can_pull(t_client, model_name, iter_counter, worker_index)
            latest_model = ps_client.pull_model(t_client, model_name, iter_counter, worker_index)
            model.linear.weight = Parameter(
                torch.from_numpy(np.asarray(latest_model[:weight_length], dtype=np.float32).reshape(weight_shape)))
            model.linear.bias = Parameter(
                torch.from_numpy(np.asarray(latest_model[weight_length:], dtype=np.float32).reshape(bias_shape[0])))
            batch_comm_time += time.time() - batch_start

            # Forward + Backward + Optimize
            batch_cal_start = time.time()
            items = Variable(items.view(-1, n_features))
            labels = Variable(labels)
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            epoch_loss += loss.item()
            loss.backward()

            # flatten and concat gradients of weight and bias
            w_b_grad = np.concatenate((model.linear.weight.grad.data.double().numpy().flatten(),
                                       model.linear.bias.grad.data.double().numpy().flatten()))
            batch_cal_time = time.time() - batch_cal_start

            # push gradient to PS
            batch_comm_start = time.time()
            ps_client.can_push(t_client, model_name, iter_counter, worker_index)
            ps_client.push_grad(t_client, model_name, w_b_grad, -1. * learning_rate / n_workers,
                                iter_counter, worker_index)
            ps_client.can_pull(t_client, model_name, iter_counter + 1, worker_index)  # sync all workers
            batch_comm_time += time.time() - batch_comm_start

            epoch_cal_time += batch_cal_time
            epoch_comm_time += batch_comm_time

            if batch_idx % 10 == 0:
                print('Epoch: [%d/%d], Batch: [%d/%d] >>> Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                      'batch cost %.4f s: cal cost %.4f s and communication cost %.4f s'
                      % (epoch + 1, n_epochs, batch_idx + 1, n_train_batch,
                         time.time() - train_start, loss.data, time.time() - epoch_start,
                         time.time() - batch_start, batch_cal_time, batch_comm_time))

            iter_counter += 1

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

        print('Epoch: [%d/%d], Batch: [%d/%d], Time: %.4f, Loss: %.4f, epoch cost %.4f: '
              'calculation cost = %.4f s, synchronization cost %.4f s, test cost %.4f s, '
              'accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (epoch + 1, n_epochs, batch_idx + 1, n_train_batch,
                 time.time() - train_start, epoch_loss, time.time() - epoch_start,
                 epoch_cal_time, epoch_comm_time, test_time,
                 n_test, 100. * n_test_correct / n_test, test_loss / n_test))

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
