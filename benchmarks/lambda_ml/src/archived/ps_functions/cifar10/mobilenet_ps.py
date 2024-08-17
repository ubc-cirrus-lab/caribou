
import sys
sys.path.append('/Users/liuyue/LambdaML')

import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from thrift_ps import constants


# algorithm setting
learning_rate = 0.1
batch_size = 200
num_epochs = 1
random_seed = 42
training_file = 'training.pt'
test_file = 'test.pt'


def handler(argv):
    from archived.pytorch_model import MobileNet
    start_time = time.time()
    worker_index = argv[1]
    num_worker = argv[2]

    print('number of workers = {}'.format(num_worker))
    print('worker index = {}'.format(worker_index))

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

    preprocess_start = time.time()
    batch_size = 200

    train_path = "../../dataset/training.pt"
    test_path = "../../dataset/test.pt"
    trainset = torch.load(train_path)
    testset = torch.load(test_path)
    train_loader = torch.utils.data.DataLoader(trainset, batch_size=batch_size, shuffle=True)

    test_loader = torch.utils.data.DataLoader(testset, batch_size=batch_size, shuffle=False)
    classes = ('plane', 'car', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck')
    device = 'cpu'
    print("preprocess data cost {} s".format(time.time() - preprocess_start))

    model = MobileNet()
    model = model.to(device)

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate, momentum=0.9, weight_decay=5e-4)

    # register model
    model_name = "mobilenet"
    parameter_shape = []
    parameter_length = []
    model_length = 0
    for param in model.parameters():
        tmp_shape = 1
        parameter_shape.append(param.data.numpy().shape)
        for w in param.data.numpy().shape:
            tmp_shape *=w
        parameter_length.append(tmp_shape)
        model_length += tmp_shape
    print("model_length = {}".format(model_length))
    model_length = 830
    """
    ps_client.register_model(t_client, worker_index, model_name, model_length, num_worker)
    #ps_client.exist_model(t_client, model_name)
    print("register and check model >>> name = {}, length = {}".format(model_name, model_length))
    """
    # Training the Model
    train_start = time.time()
    iter_counter = 0
    for epoch in range(num_epochs):
        epoch_start = time.time()
        for batch_index, (inputs, targets) in enumerate(train_loader):
            print("------worker {} epoch {} batch {}------"
                  .format(worker_index, epoch, batch_index))
            batch_start = time.time()

            # pull latest model
            ps_client.can_pull(t_client, model_name, iter_counter, worker_index)
            latest_model = ps_client.pull_model(t_client, model_name, iter_counter, worker_index)
            pos = 0
            for layer_index, param in enumerate(model.parameters()):
                param.data = Variable(torch.from_numpy(np.asarray(latest_model[pos:pos+parameter_length[layer_index]],dtype=np.float32).reshape(parameter_shape[layer_index])))
                pos += parameter_length[layer_index]

            # Forward + Backward + Optimize
            inputs, targets = inputs.to(device), targets.to(device)
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()

            # flatten and concat gradients of weight and bias
            param_grad = np.ones((1)) * (-1)
            for param in model.parameters():
                print("shape of layer = {}".format(param.data.numpy().flatten().shape))
                param_grad = np.concatenate((param_grad,param.data.numpy().flatten()))

            param_grad = np.delete(param_grad, 0)
            print("model_length = {}".format(param_grad.shape))
            cal_time = time.time() - batch_start

            # push gradient to PS
            sync_start = time.time()
            ps_client.can_push(t_client, model_name, iter_counter, worker_index)
            ps_client.push_grad(t_client, model_name, param_grad, learning_rate, iter_counter, worker_index)
            ps_client.can_pull(t_client, model_name, iter_counter+1, worker_index)      # sync all workers
            sync_time = time.time() - sync_start

            sync_time = time.time()
            print('Epoch: [%d/%d], Step: [%d/%d] >>> Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: cal cost %.4f s and communication cost %.4f s'
                  % (epoch + 1, num_epochs, batch_index + 1, len(train_loader) / batch_size,
                     time.time() - train_start, loss.data, time.time() - epoch_start,
                     time.time() - batch_start, cal_time, sync_time))
            iter_counter += 1
            test(epoch,model,test_loader,criterion,device)


def test(epoch, net, testloader, criterion, device):
    # global best_acc
    net.eval()
    test_loss = 0
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (inputs, targets) in enumerate(testloader):
            inputs, targets = inputs.to(device), targets.to(device)
            outputs = net(inputs)
            loss = criterion(outputs, targets)

            test_loss += loss.item()
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()

            # progress_bar(batch_idx, len(testloader), 'Loss: %.3f | Acc: %.3f%% (%d/%d)'
            #     % (test_loss/(batch_idx+1), 100.*correct/total, correct, total))

    # Save checkpoint.
    acc = 100.*correct/total
    print("Accuracy of epoch {} on test set is {}".format(epoch, acc))


if __name__ =="__main__":
    handler(sys.argv)
