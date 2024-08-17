import os
import torch
import torch.optim as optim
import boto3
import time
import json 

# from sync.sync_meta import SyncMeta
from archived.pytorch_model import MobileNet
from archived.functions import async_train, test

# number of epochs that can be finished within 15min
num_epoch_fn = 5

local_dir = "/tmp"

# dataset setting
training_file = 'training.pt'
test_file = 'test.pt'
checkpoint_file = 'checkpoint.pt'

# sync up mode
# sync_mode = 'cen'
# sync_mode = 'grad_avg'
sync_mode = 'model_avg'
sync_step = 39

#communication pattern
# comm_pattern = 'scatter_reduce'
#comm_pattern = 'centralized'

# learning algorithm setting
learning_rate = 0.15
batch_size = 128
num_epochs = 160


def handler(event, context):

    start_time = time.time()
    bucket = event['data_bucket']
    worker_index = event['rank']
    num_worker = event['num_workers']
    roundID = event['roundID']
    
    key = 'training_{}.pt'.format(worker_index)
    print('data_bucket = {}\n worker_index:{}\n num_worker:{}\n key:{}'.format(bucket, worker_index, num_worker, key))
    print('learning Rate: {}'.format(learning_rate))

    s3 = boto3.resource('s3')

    # read file from s3
    readS3_start = time.time()
    s3.Bucket(bucket).download_file(key, os.path.join(local_dir, training_file))
    s3.Bucket(bucket).download_file(test_file, os.path.join(local_dir, test_file))
    print("read data cost {} s".format(time.time() - readS3_start))

    trainset = torch.load(os.path.join(local_dir, training_file))
    testset = torch.load(os.path.join(local_dir, test_file))
    trainloader = torch.utils.data.DataLoader(trainset, batch_size=batch_size, shuffle=True)
    testloader = torch.utils.data.DataLoader(testset, batch_size=100, shuffle=False)
    classes = ('plane', 'car', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck')
    
    device = 'cpu'
    torch.manual_seed(1234)
    # best_acc = 0  # best test accuracy
    # start_epoch = 0  # start from epoch 0 or last checkpoint epoch

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
    
    print("Model: MobileNet, number of nodes:{}, local batch size:{}, LR:{}".format(num_worker, batch_size, learning_rate))

    net = net.to(device)
    # criterion = F.CrossEntropyLoss()
    optimizer = optim.SGD(net.parameters(), lr=learning_rate, momentum=0.9, weight_decay=5e-4)
    
    # load checkpoint if it is not the first round
    if roundID != 0:
        checked_epoch = roundID * num_epoch_fn - 1
        checked_key = '{}_{}.pt'.format(worker_index, checked_epoch)
        
        s3.Bucket('cifar10.checkpoint.3').download_file(checked_key, os.path.join(local_dir, checkpoint_file))
        checkpoint = torch.load(os.path.join(local_dir, checkpoint_file))
        
        net.load_state_dict(checkpoint['model_state_dict'])
        optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        # epoch = checkpoint['epoch']
        # loss = checkpoint['loss']

    for epoch in range(num_epoch_fn):
        epoch_global = roundID * num_epoch_fn + epoch

        train_loss, train_acc = async_train(epoch_global, net, trainloader, optimizer, device, worker_index, num_worker, sync_mode, sync_step)
        test_loss, test_acc = test(epoch_global, net, testloader, device)
        
        print('Epoch: {}/{},'.format(epoch_global+1, num_epochs),
              'train loss: {}'.format(train_loss),
              'train acc: {},'.format(train_acc),
              'test loss: {}'.format(test_loss),
              'test acc: {}.'.format(test_acc),)

        # training is finished
        if epoch_global == num_epochs-1:
            print("Complete {} epochs!".format(num_epochs))
            return 0
        # this round is finished, invoke next round
        elif epoch == num_epoch_fn - 1:
            checkpoint = {
                'epoch': epoch_global,
                'model_state_dict': net.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'loss': train_loss
            }
            
            torch.save(checkpoint, os.path.join(local_dir, checkpoint_file))
            # format of checkpoint: workerID_epochID
            s3.Bucket('cifar10.checkpoint.3').upload_file(os.path.join(local_dir, checkpoint_file), '{}_{}.pt'.format(worker_index, epoch_global))
            print("Epoch {} in Round {} saved!".format(epoch_global+1, roundID))

            print("Invoking the next round of functions. RoundID:{}".format(event['roundID']+1))
            lambda_client = boto3.client('lambda')
            payload = {
               'data_bucket': event['data_bucket'],
               'num_workers': event['num_workers'],
               'rank': event['rank'],
               'roundID': event['roundID']+1
            }
            lambda_client.invoke(FunctionName='async_mobile_3', InvocationType='Event', Payload=json.dumps(payload))
