import time

import torch
import torch.nn as nn
import torch.optim as optim
from torch.autograd import Variable

from archived.s3 import download_file

from archived.elasticache.Memcached import memcached_init
from archived.elasticache.Memcached import hset_object

local_dir = "/tmp"

# dataset setting
training_file = "training.pt"
test_file = "test.pt"

# sync up mode
sync_mode = 'grad_avg'
#sync_mode = 'model_avg'
sync_step = 1

# learning algorithm setting
learning_rate = 0.01
batch_size = 200
num_epochs = 1

merged_bucket = "merged-value-2"
tmp_bucket = "tmp-value-2"
weights_prefix = 'w_'
gradients_prefix = 'g_'


def handler(event, context):
    start_time = time.time()
    #bucket = "cifar10dataset"
    bucket = event['data_bucket']
    worker_index = event['rank']
    elasti_location = event['elasticache']
    endpoint = memcached_init(elasti_location)
    #endpoint = elasti_location
    #worker_index = 0
    num_worker = event['num_workers']
    #num_worker = 10
    key = 'training_{}.pt'.format(worker_index)
    print('data_bucket = {}\n worker_index:{}\n num_worker:{}\n key:{}'.format(bucket, worker_index, num_worker, key))

    # read file from s3
    readS3_start = time.time()

    train_path = download_file(bucket, key)
    test_path = download_file(bucket, test_file)
    trainset = torch.load(train_path)
    testset= torch.load(test_path)
    print("read data cost {} s".format(time.time() - readS3_start))
    #print(trainset)
    batch_size = 200
    batch_size = int(np.ceil(batch_size/num_worker))
    trainloader = torch.utils.data.DataLoader(trainset, batch_size=batch_size, shuffle=True)

    testloader = torch.utils.data.DataLoader(testset, batch_size=100, shuffle=False)
    classes = ('plane', 'car', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck')
    device = 'cpu'
    # best_acc = 0  # best test accuracy
    # start_epoch = 0  # start from epoch 0 or last checkpoint epoch
    #Model
    print('==> Building model..')
    # net = VGG('VGG19')
    # net = ResNet18()
    #net = ResNet50()
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

    print("Model: MobileNet")

    net = net.to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(net.parameters(), lr=learning_rate, momentum=0.9, weight_decay=5e-4)

    for epoch in range(num_epochs):
        train(endpoint, epoch, net, trainloader, optimizer, criterion, device, worker_index, num_worker, sync_mode, sync_step)
        test(epoch, net, testloader, criterion, device)

    #put_object("time-record-s3", "time_{}".format(worker_index), pickle.dumps(time_record))


# Training
def train(endpoint, epoch, net, trainloader, optimizer, criterion, device, worker_index, num_worker, sync_mode, sync_step):

    # print('\nEpoch: %d' % epoch)
    net.train()
    # train_loss = 0
    # correct = 0
    # total = 0
    end = 0
    batch_start = time.time()
    for batch_idx, (inputs, targets) in enumerate(trainloader):

        print("------worker {} epoch {} batch {}------".format(worker_index, epoch+1, batch_idx+1))

        inputs, targets = inputs.to(device), targets.to(device)
        optimizer.zero_grad()
        outputs = net(inputs)
        loss = criterion(outputs, targets)
        loss.backward()

        if sync_mode == 'grad_avg':
            sync_start = time.time()
            gradients = [param.grad.data.numpy() for param in net.parameters()]
            # print("[Worker {}] Gradients before sync = {}".format(worker_index, gradients[0][0]))
            put_object_start = time.time()
            sync_start = time.time()
            print(hset_object(endpoint, tmp_bucket, gradients_prefix + str(worker_index), pickle.dumps(gradients)))
            tmp_write_local_epoch_time = time.time()-put_object_start
            print("writing local gradients in elasticache cost {}".format(tmp_write_local_epoch_time))

            file_postfix = "{}_{}".format(epoch, batch_idx)
            if worker_index == 0:
                # merge all workers
                merged_value = merge_w_b_layers(endpoint, tmp_bucket, num_worker, gradients_prefix)

                # upload merged value to elasticache
                put_merged_w_b_layers(endpoint, merged_bucket, merged_value, gradients_prefix, file_postfix)
                #if batch_idx > end:
                #    end = end+1
                #delete_expired_w_b_layers(endpoint, merged_bucket, epoch, batch_idx, gradients_prefix,end)
            else:
                # get merged value from redis
                merged_value = get_merged_w_b_layers(endpoint, merged_bucket, gradients_prefix, file_postfix)

            for layer_index, param in enumerate(net.parameters()):
                param.grad = Variable(torch.from_numpy(merged_value[layer_index]))

        tmp_sync_time = time.time() - sync_start
        print("synchronization cost {} s".format(tmp_sync_time))

        optimizer.step()

        print("batch cost = {}".format(time.time()-batch_start))
        batch_start = time.time()
        # train_loss += loss.item()
        # _, predicted = outputs.max(1)
        # total += targets.size(0)
        # correct += predicted.eq(targets).sum().item()
        #print("batch cost {} s".format(time.time() - batch_start))
        if (batch_idx + 1) % 1 == 0:
            print('Epoch: {}, Step: {}, Loss:{}'.format(epoch+1, batch_idx+1, loss.data))
    #return sync_epoch_time,write_local_epoch_time,calculation_epoch_time


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

    # Save checkpoint.
    acc = 100.*correct/total
    print("Accuracy of epoch {} on test set is {}".format(epoch, acc))
