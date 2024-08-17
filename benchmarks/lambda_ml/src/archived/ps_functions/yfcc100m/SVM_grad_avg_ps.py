import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.nn import Parameter
from torch.utils.data.sampler import SubsetRandomSampler

from archived.s3.get_object import get_object

from archived.old_model.SVM import SVM
from data_loader.YFCCLibsvmDataset import DenseLibsvmDataset

from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


# algorithm setting
NUM_FEATURES = 30
NUM_CLASSES = 2
LEARNING_RATE = 0.1
BATCH_SIZE = 10000
NUM_EPOCHS = 10
VALIDATION_RATIO = .2
SHUFFLE_DATASET = True
RANDOM_SEED = 42


def handler(event, context):
    start_time = time.time()
    bucket = event['bucket_name']
    worker_index = event['rank']
    num_workers = event['num_workers']
    key = event['file'].split(",")
    num_classes = event['num_classes']
    num_features = event['num_features']
    pos_tag = event['pos_tag']
    num_epochs = event['num_epochs']
    learning_rate = event['learning_rate']
    batch_size = event['batch_size']
    host = event['host']
    port = event['port']

    print('bucket = {}'.format(bucket))
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print("file = {}".format(key))
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('num epochs = {}'.format(num_epochs))
    print('num classes = {}'.format(num_classes))
    print('num features = {}'.format(num_features))
    print('positive tag = {}'.format(pos_tag))
    print('learning rate = {}'.format(learning_rate))
    print("batch_size = {}".format(batch_size))
    print("host = {}".format(host))
    print("port = {}".format(port))

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
    split = int(np.floor(VALIDATION_RATIO * dataset_size))
    if SHUFFLE_DATASET:
        np.random.seed(RANDOM_SEED)
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
    model = SVM(NUM_FEATURES, NUM_CLASSES)

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=LEARNING_RATE)

    # register model
    model_name = "w.b"
    weight_shape = model.linear.weight.data.numpy().shape
    weight_length = weight_shape[0] * weight_shape[1]
    bias_shape = model.linear.bias.data.numpy().shape
    bias_length = bias_shape[0]
    model_length = weight_length + bias_length
    ps_client.register_model(t_client, worker_index, model_name, model_length, num_workers)
    ps_client.exist_model(t_client, model_name)
    print("register and check model >>> name = {}, length = {}".format(model_name, model_length))

    # Training the Model
    train_start = time.time()
    iter_counter = 0
    for epoch in range(NUM_EPOCHS):
        epoch_start = time.time()
        for batch_index, (items, labels) in enumerate(train_loader):
            print("------worker {} epoch {} batch {}------"
                  .format(worker_index, epoch, batch_index))
            batch_start = time.time()

            # pull latest model
            ps_client.can_pull(t_client, model_name, iter_counter, worker_index)
            latest_model = ps_client.pull_model(t_client, model_name, iter_counter, worker_index)
            model.linear.weight = Parameter(torch.from_numpy(np.asarray(latest_model[:weight_length],dtype=np.double).reshape(weight_shape)))
            model.linear.bias = Parameter(torch.from_numpy(np.asarray(latest_model[weight_length:], dtype=np.double).reshape(bias_shape[0])))

            items = Variable(items.view(-1, NUM_FEATURES))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items.double())
            loss = criterion(outputs, labels)
            loss.backward()

            # flatten and concat gradients of weight and bias
            w_b_grad = np.concatenate((model.linear.weight.grad.data.numpy().flatten(),
                                       model.linear.bias.grad.data.numpy().flatten()))
            cal_time = time.time() - batch_start

            # push gradient to PS
            sync_start = time.time()
            ps_client.can_push(t_client, model_name, iter_counter, worker_index)
            ps_client.push_grad(t_client, model_name, w_b_grad, LEARNING_RATE, iter_counter, worker_index)
            ps_client.can_pull(t_client, model_name, iter_counter + 1, worker_index)  # sync all workers
            sync_time = time.time() - sync_start

            print('Epoch: [%d/%d], Step: [%d/%d] >>> Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: cal cost %.4f s and communication cost %.4f s'
                  % (epoch + 1, NUM_EPOCHS, batch_index + 1, len(train_indices) / BATCH_SIZE,
                     time.time() - train_start, loss.data, time.time() - epoch_start,
                     time.time() - batch_start, cal_time, sync_time))
            iter_counter += 1

        # Test the Model
        correct = 0
        total = 0
        test_loss = 0
        for items, labels in validation_loader:
            items = Variable(items.view(-1, NUM_FEATURES))
            labels = Variable(labels)
            outputs = model(items)
            test_loss += criterion(outputs, labels).data
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()

        print('Time = %.4f, accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (time.time() - train_start, len(val_indices), 100 * correct / total, test_loss))

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
