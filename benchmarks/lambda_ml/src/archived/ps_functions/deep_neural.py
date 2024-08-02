import time
import os
# import urllib.parse
import numpy as np
import boto3

import torch
import torch.optim as optim
import torch.nn.functional as F

from torch.utils.data.sampler import SubsetRandomSampler

# from sync.sync_meta import SyncMeta

from archived.pytorch_model import MobileNet
# from training_test import train, test

# from sync.sync_grad import *
# from sync.sync_reduce_scatter import reduce_scatter_batch_multi_bucket, delete_expired_merged

# from model.LogisticRegression import LogisticRegression
# from data_loader.LibsvmDataset import DenseLibsvmDataset2

from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from thrift_ps import constants


# algorithm setting
# NUM_FEATURES = 30
# NUM_CLASSES = 2
# LEARNING_RATE = 0.1
# BATCH_SIZE = 10000
NUM_EPOCHS = 10
# VALIDATION_RATIO = .2
# SHUFFLE_DATASET = True
# RANDOM_SEED = 42

# dataset setting
training_file = 'training.pt'
test_file = 'test.pt'
checkpoint_file = 'checkpoint.pt'
local_dir = "/tmp"

# sync up mode
# sync_mode = 'cen'
# sync_mode = 'grad_avg'
# sync_mode = 'model_avg'
# sync_step = 39

# learning algorithm setting
learning_rate = 0.1
batch_size = 128
# num_epochs = 80

s3 = boto3.resource('s3')

class Accuracy(object):

    def __init__(self):
        self.correct = 0
        self.count = 0

    def __str__(self):
        return '{:.2f}%'.format(self.accuracy * 100)

    @property
    def accuracy(self):
        return self.correct / self.count

    def update(self, output, target):
        with torch.no_grad():
            pred = output.argmax(dim=1)
            correct = pred.eq(target).sum().item()

        self.correct += correct
        self.count += output.size(0)
        
class Average(object):

    def __init__(self):
        self.sum = 0
        self.count = 0

    def __str__(self):
        return '{:.6f}'.format(self.average)

    @property
    def average(self):
        return self.sum / self.count

    def update(self, value, number):
        self.sum += value * number
        self.count += number

def handler(event, context):
    start_time = time.time()
    bucket = event['data_bucket']
    worker_index = event['rank']
    num_workers = event['num_workers']
    key = 'training_{}.pt'.format(worker_index)

    print('data_bucket = {}\n worker_index:{}\n num_worker:{}\n key:{}'.format(bucket, worker_index, num_workers, key))

    # Set thrift connection
    # Make socket
    transport = TSocket.TSocket(constants.HOST, constants.PORT)
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
    print("create and ping thrift server >>> HOST = {}, PORT = {}"
          .format(constants.HOST, constants.PORT))

    # read file from s3
    readS3_start = time.time()
    s3.Bucket(bucket).download_file(key, os.path.join(local_dir, training_file))
    s3.Bucket(bucket).download_file(test_file, os.path.join(local_dir, test_file))
    print("read data cost {} s".format(time.time() - readS3_start))


    # preprocess dataset
    preprocess_start = time.time()

    trainset = torch.load(os.path.join(local_dir, training_file))
    testset = torch.load(os.path.join(local_dir, test_file))
    trainloader = torch.utils.data.DataLoader(trainset, batch_size=batch_size, shuffle=True)
    testloader = torch.utils.data.DataLoader(testset, batch_size=128, shuffle=False)
    classes = ('plane', 'car', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck')
    print("preprocess data cost {} s".format(time.time() - preprocess_start))

    device = 'cpu'
    torch.manual_seed(1234)

    #Model
    print('==> Building model..')
    # net = VGG('VGG19')
    # net = ResNet18()
    # net = ResNet50()
    # net = PreActResNet18()
    # net = GoogLeNet()
    # net = DenseNet121()
    # net = ResNeXt29_2x64d()
    net = MobileNet()
    # net = MobileNetV2()
    # net = DPN92()
    # net = ShuffleNetG2()
    # net = SENet18()
    # net = ShuffleNetV2(1)
    # net = EfficientNetB0()

    net = net.to(device)

    optimizer = optim.SGD(net.parameters(), lr=learning_rate, momentum=0.9, weight_decay=5e-4)


    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    # criterion = torch.nn.CrossEntropyLoss()
    # optimizer = torch.optim.SGD(model.parameters(), lr=LEARNING_RATE)

    # register model
    model_name = "dnn"
    weight = [param.data.numpy() for param in net.parameters()]
    weight_shape = [layer.shape for layer in weight]
    weight_size = [layer.size for layer in weight]
    weight_length = sum(weight_size)

    ps_client.register_model(t_client, worker_index, model_name, weight_length, num_workers)
    ps_client.exist_model(t_client, model_name)

    print("register and check model >>> name = {}, length = {}".format(model_name, weight_length))

    # Training the Model
    train_start = time.time()
    iter_counter = 0

    for epoch in range(NUM_EPOCHS):

        epoch_start = time.time()

        net.train()
        num_batch = 0
        train_acc = Accuracy()
        train_loss = Average()

        for batch_idx, (inputs, targets) in enumerate(trainloader):

            # print("------worker {} epoch {} batch {}------".format(worker_index, epoch+1, batch_idx+1))
            batch_start = time.time()
            
            # pull latest model
            pull_start = time.time()
            ps_client.can_pull(t_client, model_name, iter_counter, worker_index)
            latest_model = ps_client.pull_model(t_client, model_name, iter_counter, worker_index)
            latest_model = np.asarray(latest_model,dtype=np.float32)
            pull_time = time.time() - pull_start

            # update the model
            offset = 0
            for layer_index, param in enumerate(net.parameters()):

                layer_value = latest_model[offset : offset + weight_size[layer_index]].reshape(weight_shape[layer_index])
                param.data = torch.from_numpy(layer_value)

                offset += weight_size[layer_index]
    
            # Forward + Backward + Optimize
            inputs, targets = inputs.to(device), targets.to(device)
            outputs = net(inputs)
            loss = F.cross_entropy(outputs, targets)
            
            optimizer.zero_grad()
            loss.backward()
            
            train_acc.update(outputs, targets)
            train_loss.update(loss.item(), inputs.size(0))
            
            # flatten and concat gradients of weight and bias
            for index, param in enumerate(net.parameters()):
                if index == 0:
                    flattened_grad = param.grad.data.numpy().flatten()
                else:
                    flattened_grad = np.concatenate((flattened_grad, param.grad.data.numpy().flatten()))
    
            flattened_grad = flattened_grad * -1
            
            # push gradient to PS
            push_start = time.time()
            ps_client.can_push(t_client, model_name, iter_counter, worker_index)
            ps_client.push_grad(t_client, model_name, flattened_grad, learning_rate, iter_counter, worker_index)
            ps_client.can_pull(t_client, model_name, iter_counter+1, worker_index)      # sync all workers
            push_time = time.time() - push_start

            iter_counter += 1
            num_batch += 1
            
            step_time = time.time() - batch_start
            
            print("Epoch:[{}/{}], Step:[{}/{}];\n Training Loss:{}, Training accuracy:{};\n Step Time:{}, Calculation Time:{}, Communication Time:{}".format(
                epoch, NUM_EPOCHS, num_batch, len(trainloader), train_loss, train_acc, step_time, step_time - (pull_time + push_time), pull_time + push_time))

        # Test the Model
        net.eval()
        test_loss = Average()
        test_acc = Accuracy()
        
        with torch.no_grad():
            for batch_idx, (inputs, targets) in enumerate(testloader):
                
                inputs, targets = inputs.to(device), targets.to(device)
                
                outputs = net(inputs)
                
                loss = F.cross_entropy(outputs, targets)
                
                test_loss.update(loss.item(), inputs.size(0))
                test_acc.update(outputs, targets)
        # correct = 0
        # total = 0
        # test_loss = 0
        # for items, labels in validation_loader:
        #     items = Variable(items.view(-1, NUM_FEATURES))
        #     labels = Variable(labels)
        #     outputs = model(items)
        #     test_loss += criterion(outputs, labels).data
        #     _, predicted = torch.max(outputs.data, 1)
        #     total += labels.size(0)
        #     correct += (predicted == labels).sum()

        print('Time = %.4f, accuracy of the model on test set: %f, loss = %f'
              % (time.time() - train_start, test_acc, test_loss))

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
