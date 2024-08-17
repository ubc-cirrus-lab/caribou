import os
import time
import math

import numpy as np
import pickle
import json

import torch
from torch.autograd import Variable
import torch.nn.functional as F

import boto3

from utils.constants import Prefix, MLModel, Optimization, Synchronization
from src.storage import S3Storage

from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from model import deep_models
from utils.metric import Accuracy, Average


def handler(event, context):
    start_time = time.time()

    # dataset setting
    train_file = event['train_file']
    test_file = event['test_file']
    data_bucket = event['data_bucket']
    n_features = event['n_features']
    n_classes = event['n_classes']
    n_workers = event['n_workers']
    worker_index = event['worker_index']
    cp_bucket = event['cp_bucket']

    # ps setting
    host = event['host']
    port = event['port']

    # training setting
    model_name = event['model']
    optim = event['optim']
    sync_mode = event['sync_mode']
    assert model_name.lower() in MLModel.Deep_Models
    assert optim.lower() in Optimization.Grad_Avg
    assert sync_mode.lower() in Synchronization.Reduce

    # hyper-parameter
    learning_rate = event['lr']
    batch_size = event['batch_size']
    n_epochs = event['n_epochs']
    start_epoch = event['start_epoch']
    run_epochs = event['run_epochs']

    function_name = event['function_name']

    print('data bucket = {}'.format(data_bucket))
    print("train file = {}".format(train_file))
    print("test file = {}".format(test_file))
    print('number of workers = {}'.format(n_workers))
    print('worker index = {}'.format(worker_index))
    print('model = {}'.format(model_name))
    print('optimization = {}'.format(optim))
    print('sync mode = {}'.format(sync_mode))
    print('start epoch = {}'.format(start_epoch))
    print('run epochs = {}'.format(run_epochs))
    print('host = {}'.format(host))
    print('port = {}'.format(port))

    print("Run function {}, round: {}/{}, epoch: {}/{} to {}/{}"
          .format(function_name, int(start_epoch/run_epochs) + 1, math.ceil(n_epochs / run_epochs),
                  start_epoch + 1, n_epochs, start_epoch + run_epochs, n_epochs))

    # download file from s3
    storage = S3Storage()
    local_dir = "/tmp"
    read_start = time.time()
    storage.download(data_bucket, train_file, os.path.join(local_dir, train_file))
    storage.download(data_bucket, test_file, os.path.join(local_dir, test_file))
    print("download file from s3 cost {} s".format(time.time() - read_start))

    train_set = torch.load(os.path.join(local_dir, train_file))
    test_set = torch.load(os.path.join(local_dir, test_file))
    train_loader = torch.utils.data.DataLoader(train_set, batch_size=batch_size, shuffle=True)
    n_train_batch = len(train_loader)
    test_loader = torch.utils.data.DataLoader(test_set, batch_size=100, shuffle=False)
    classes = ('plane', 'car', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck')

    print("read data cost {} s".format(time.time() - read_start))

    random_seed = 100
    torch.manual_seed(random_seed)

    device = 'cpu'
    model = deep_models.get_models(model_name).to(device)
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # load checkpoint model if it is not the first round
    if start_epoch != 0:
        checked_file = 'checkpoint_{}.pt'.format(start_epoch - 1)
        storage.download(cp_bucket, checked_file, os.path.join(local_dir, checked_file))
        checkpoint_model = torch.load(os.path.join(local_dir, checked_file))

        model.load_state_dict(checkpoint_model['model_state_dict'])
        optimizer.load_state_dict(checkpoint_model['optimizer_state_dict'])
        print("load checkpoint model at epoch {}".format(start_epoch - 1))

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

    # register model
    parameter_shape = []
    parameter_length = []
    model_length = 0
    for param in model.parameters():
        tmp_shape = 1
        parameter_shape.append(param.data.numpy().shape)
        for w in param.data.numpy().shape:
            tmp_shape *= w
        parameter_length.append(tmp_shape)
        model_length += tmp_shape

    ps_client.register_model(t_client, worker_index, model_name, model_length, n_workers)
    ps_client.exist_model(t_client, model_name)
    print("register and check model >>> name = {}, length = {}".format(model_name, model_length))

    # Training the Model
    train_start = time.time()
    iter_counter = 0
    for epoch in range(start_epoch, min(start_epoch + run_epochs, n_epochs)):

        model.train()
        epoch_start = time.time()

        train_acc = Accuracy()
        train_loss = Average()

        for batch_idx, (inputs, targets) in enumerate(train_loader):
            batch_start = time.time()
            batch_cal_time = 0
            batch_comm_time = 0

            # pull latest model
            ps_client.can_pull(t_client, model_name, iter_counter, worker_index)
            latest_model = ps_client.pull_model(t_client, model_name, iter_counter, worker_index)
            pos = 0
            for layer_index, param in enumerate(model.parameters()):
                param.data = Variable(torch.from_numpy(
                    np.asarray(latest_model[pos:pos + parameter_length[layer_index]], dtype=np.float32)
                        .reshape(parameter_shape[layer_index])))
                pos += parameter_length[layer_index]
            batch_comm_time += time.time() - batch_start

            batch_cal_start = time.time()
            outputs = model(inputs)
            loss = F.cross_entropy(outputs, targets)
            optimizer.zero_grad()
            loss.backward()

            # flatten and concat gradients of weight and bias
            param_grad = np.zeros((1))
            for param in model.parameters():
                # print("shape of layer = {}".format(param.data.numpy().flatten().shape))
                param_grad = np.concatenate((param_grad, param.data.numpy().flatten()))
            param_grad = np.delete(param_grad, 0)
            #print("model_length = {}".format(param_grad.shape))
            batch_cal_time += time.time() - batch_cal_start

            # push gradient to PS
            batch_push_start = time.time()
            ps_client.can_push(t_client, model_name, iter_counter, worker_index)
            ps_client.push_grad(t_client, model_name, param_grad, -1. * learning_rate / n_workers,
                                iter_counter, worker_index)
            ps_client.can_pull(t_client, model_name, iter_counter + 1, worker_index)  # sync all workers
            batch_comm_time += time.time() - batch_push_start

            train_acc.update(outputs, targets)
            train_loss.update(loss.item(), inputs.size(0))

            optimizer.step()
            iter_counter += 1

            if batch_idx % 10 == 0:
                print('Epoch: [%d/%d], Batch: [%d/%d], Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                      'batch cost %.4f s: cal cost %.4f s and communication cost %.4f s'
                      % (epoch + 1, n_epochs, batch_idx + 1, n_train_batch,
                         time.time() - train_start, loss.item(), time.time() - epoch_start,
                         time.time() - batch_start, batch_cal_time, batch_comm_time))

        test_loss, test_acc = test(epoch, model, test_loader)

        print('Epoch: {}/{},'.format(epoch + 1, n_epochs),
              'train loss: {},'.format(train_loss),
              'train acc: {},'.format(train_acc),
              'test loss: {},'.format(test_loss),
              'test acc: {}.'.format(test_acc), )

    # training is not finished yet, invoke next round
    if epoch < n_epochs - 1:
        checkpoint_model = {
            'epoch': epoch,
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'loss': train_loss.average
        }

        checked_file = 'checkpoint_{}.pt'.format(epoch)

        if worker_index == 0:
            torch.save(checkpoint_model, os.path.join(local_dir, checked_file))
            storage.upload(cp_bucket, checked_file, os.path.join(local_dir, checked_file))
            print("checkpoint model at epoch {} saved!".format(epoch))

        print("Invoking the next round of functions. round: {}/{}, start epoch: {}, run epoch: {}"
              .format(int((epoch + 1) / run_epochs) + 1, math.ceil(n_epochs / run_epochs),
                      epoch + 1, run_epochs))
        lambda_client = boto3.client('lambda')
        payload = {
            'train_file': event['train_file'],
            'test_file': event['test_file'],
            'data_bucket': event['data_bucket'],
            'n_features': event['n_features'],
            'n_classes': event['n_classes'],
            'n_workers': event['n_workers'],
            'worker_index': event['worker_index'],
            'cp_bucket': event['cp_bucket'],
            'host': event['host'],
            'port': event['port'],
            'model': event['model'],
            'optim': event['optim'],
            'sync_mode': event['sync_mode'],
            'lr': event['lr'],
            'batch_size': event['batch_size'],
            'n_epochs': event['n_epochs'],
            'start_epoch': epoch + 1,
            'run_epochs': event['run_epochs'],
            'function_name': event['function_name']
        }
        lambda_client.invoke(FunctionName=function_name,
                             InvocationType='Event',
                             Payload=json.dumps(payload))

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))


def test(epoch, net, test_loader):
    # global best_acc
    net.eval()
    test_loss = Average()
    test_acc = Accuracy()

    with torch.no_grad():
        for batch_idx, (inputs, targets) in enumerate(test_loader):
            outputs = net(inputs)

            loss = F.cross_entropy(outputs, targets)

            test_loss.update(loss.item(), inputs.size(0))
            test_acc.update(outputs, targets)

    return test_loss, test_acc
