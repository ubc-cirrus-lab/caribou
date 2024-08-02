import time
import pickle

import torch
import torch.nn as nn
import torch.optim as optim
import torchvision

local_dir = "/tmp"




# learning algorithm setting
learning_rate = 0.01
batch_size = 20
num_epochs = 1


def handler(event, context):

    startTs = time.time()
    trainset = torchvision.datasets.CIFAR10("../dataset",train=True,download=True)
    testset = torchvision.datasets.CIFAR10("../dataset",train=False,download=True)
    
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
        #test(epoch, net, testloader, criterion, device)
    
    put_object("time-record-s3","time_{}".format(worker_index),pickle.dumps(time_record))
# Training
def train(endpoint, epoch, net, trainloader, optimizer, criterion, device, worker_index, num_worker, sync_mode, sync_step):
    
    # print('\nEpoch: %d' % epoch)
    net.train()
    # train_loss = 0
    # correct = 0
    # total = 0
    batch_start = time.time() 
    for batch_idx, (inputs, targets) in enumerate(trainloader):

        print("------worker {} epoch {} batch {}------".format(worker_index, epoch+1, batch_idx+1))
        
        inputs, targets = inputs.to(device), targets.to(device)
        optimizer.zero_grad()
        outputs = net(inputs)
        loss = criterion(outputs, targets)
        loss.backward()
        optimizer.step()
    print("batch time = {}".format(time.time()-batch_start))
            
    batch_start = time.time()    
            


