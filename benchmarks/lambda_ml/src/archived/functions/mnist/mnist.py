import time
import os

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler
import torchvision.transforms as transforms
import torchvision.datasets as dsets

from archived.old_model import LogisticRegression
from archived.sync import SyncMeta


# lambda setting
grad_bucket = "tmp-grads"
model_bucket = "tmp-updates"
local_dir = "/tmp"
w_prefix = "w_"
b_prefix = "b_"
w_grad_prefix = "w_grad_"
b_grad_prefix = "b_grad_"

# dataset setting
processed_folder = 'processed'
training_file = 'training.pt'
test_file = 'test.pt'
num_features = 784
num_classes = 10

# sync up mode
sync_mode = 'gradient_avg'
sync_step = 50

# learning algorithm setting
learning_rate = 0.001
batch_size = 100
num_epochs = 3

# validation_ratio = .2
# shuffle_dataset = True
# random_seed = 42
s3 = boto3.resource('s3')

def handler(event, context):
    
    startTs = time.time()
    bucket = event['data_bucket']
    worker_index = event['rank']
    num_worker = event['num_workers']
    key = 'training_{}.pt'.format(worker_index)
    print('data_bucket = {}\n worker_index:{}\n num_worker:{}\n key:{}'.format(bucket, worker_index, num_worker, key))
    sync_meta = SyncMeta(worker_index, num_worker)
    print("synchronization meta {}".format(sync_meta.__str__()))

    # read file from s3
    readS3_start = time.time()
    if not os.path.exists(os.path.join(local_dir, processed_folder)):
        os.makedirs(os.path.join(local_dir, processed_folder))
    
    s3.Bucket(bucket).download_file(key, os.path.join(local_dir, processed_folder, training_file))
    s3.Bucket(bucket).download_file(test_file, os.path.join(local_dir, processed_folder, test_file))
    print("read data cost {} s".format(time.time() - readS3_start))

    # load dataset
    train_dataset = dsets.MNIST(root=local_dir, train=True, transform=transforms.ToTensor(), download=False)
    test_dataset = dsets.MNIST(root=local_dir, train=False, transform=transforms.ToTensor(), download=False)
    # print('[{}]length of training:{}, length of test:{}'.format(worker_index, len(train_dataset), len(test_dataset)))
    train_loader = torch.utils.data.DataLoader(dataset=train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = torch.utils.data.DataLoader(dataset=test_dataset, batch_size=batch_size, shuffle=False)

    model = LogisticRegression(num_features, num_classes)
    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # # Training the Model
    step = 0
    for epoch in range(num_epochs):
        for batch_index, (items, labels) in enumerate(train_loader):
            
            print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
            batch_start = time.time()
            images = Variable(items.view(-1, 28 * 28))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(images)
            loss = criterion(outputs, labels)
            loss.backward()
            print("forward and backward cost {} s".format(time.time()-batch_start))
            
            if sync_mode == 'model_avg':

                # apply local gradient to local model
                optimizer.step()
                # average model
                if (step+1) % sync_step == 0:
                    
                    w_grad = model.linear.weight.data.numpy()
                    b_grad = model.linear.bias.data.numpy()
                    #print("dtype of grad = {}".format(w_grad.dtype))
                    # print("w_grad before merge = {}".format(w_grad[0][0:5]))
                    # print("b_grad before merge = {}".format(b_grad))

                    sync_start = time.time()
                    put_object(grad_bucket, w_grad_prefix + str(worker_index), w_grad.tobytes())
                    put_object(grad_bucket, b_grad_prefix + str(worker_index), b_grad.tobytes())

                    file_postfix = "{}_{}".format(epoch, batch_index)
                    if worker_index == 0:
                        w_grad_merge, b_grad_merge = \
                            merge_w_b_grads(grad_bucket, num_worker, w_grad.dtype,
                                            w_grad.shape, b_grad.shape,
                                            w_grad_prefix, b_grad_prefix)
                        put_merged_w_b_grad(model_bucket, w_grad_merge, b_grad_merge,
                                            file_postfix, w_grad_prefix, b_grad_prefix)
                        delete_expired_w_b(model_bucket, epoch, batch_index, w_grad_prefix, b_grad_prefix)

                        # update the model with averaged model
                        model.linear.weight = torch.nn.Parameter(torch.from_numpy(w_grad_merge).mul_(1/num_worker))
                        model.linear.bias = torch.nn.Parameter(torch.from_numpy(b_grad_merge).mul_(1/num_worker))
                    else:
                        w_grad_merge, b_grad_merge = get_merged_w_b_grad(model_bucket, file_postfix,
                                                                        w_grad.dtype, w_grad.shape, b_grad.shape,
                                                                        w_grad_prefix, b_grad_prefix)
                        # update the model with averaged model
                        model.linear.weight = torch.nn.Parameter(torch.from_numpy(w_grad_merge).mul_(1/num_worker))
                        model.linear.bias = torch.nn.Parameter(torch.from_numpy(b_grad_merge).mul_(1/num_worker))
                        
                    print("synchronization cost {} s".format(time.time() - sync_start))
                    print("batch cost {} s".format(time.time() - batch_start))

                    # print("w_grad after merge = {}".format(model.linear.weight.data.numpy()[0][:5]))
                    # print("b_grad after merge = {}".format(model.linear.bias.data.numpy()))
                    
            if sync_mode == 'gradient_avg':
                w_grad = model.linear.weight.grad.data.numpy()
                b_grad = model.linear.bias.grad.data.numpy()
                #print("dtype of grad = {}".format(w_grad.dtype))
                # print("w_grad before merge = {}".format(w_grad[0][0:5]))
                # print("b_grad before merge = {}".format(b_grad))

                sync_start = time.time()
                put_object(grad_bucket, w_grad_prefix + str(worker_index), w_grad.tobytes())
                put_object(grad_bucket, b_grad_prefix + str(worker_index), b_grad.tobytes())

                file_postfix = "{}_{}".format(epoch, batch_index)
                if worker_index == 0:
                    w_grad_merge, b_grad_merge = \
                        merge_w_b_grads(grad_bucket, num_worker, w_grad.dtype,
                                        w_grad.shape, b_grad.shape,
                                        w_grad_prefix, b_grad_prefix)
                    put_merged_w_b_grad(model_bucket, w_grad_merge, b_grad_merge,
                                        file_postfix, w_grad_prefix, b_grad_prefix)
                    delete_expired_w_b(model_bucket, epoch, batch_index, w_grad_prefix, b_grad_prefix)
                    model.linear.weight.grad = Variable(torch.from_numpy(w_grad_merge))
                    model.linear.bias.grad = Variable(torch.from_numpy(b_grad_merge))
                else:
                    w_grad_merge, b_grad_merge = get_merged_w_b_grad(model_bucket, file_postfix,
                                                                    w_grad.dtype, w_grad.shape, b_grad.shape,
                                                                    w_grad_prefix, b_grad_prefix)
                    model.linear.weight.grad = Variable(torch.from_numpy(w_grad_merge))
                    model.linear.bias.grad = Variable(torch.from_numpy(b_grad_merge))

                print("synchronization cost {} s".format(time.time() - sync_start))
                # print("w_grad after merge = {}".format(model.linear.weight.grad.data.numpy()[0][:5]))
                # print("b_grad after merge = {}".format(model.linear.bias.grad.data.numpy()))

                optimizer.step()

                print("batch cost {} s".format(time.time() - batch_start))

            if (batch_index + 1) % 10 == 0:
                print('Epoch: [%d/%d], Step: [%d/%d], Loss: %.4f'
                    % (epoch + 1, num_epochs, batch_index + 1, len(train_dataset) / batch_size, loss.data))
            
            step += 1
            
        # Test the Model
        correct = 0
        total = 0
        for items, labels in test_loader:
            # items = Variable(items.view(-1, num_features))
            # items = Variable(items)
    
            images = Variable(items.view(-1, 28 * 28))
            labels = Variable(labels)
    
            outputs = model(images)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()
    
        print('Accuracy of the model on the %d test samples: %d %%' % (len(test_dataset), 100 * correct / total))


    if worker_index == 0:
        clear_bucket(model_bucket)
        clear_bucket(grad_bucket)

    # Test the Model
    correct = 0
    total = 0
    for items, labels in test_loader:
        # items = Variable(items.view(-1, num_features))
        # items = Variable(items)

        images = Variable(items.view(-1, 28 * 28))
        labels = Variable(labels)

        outputs = model(images)
        _, predicted = torch.max(outputs.data, 1)
        total += labels.size(0)
        correct += (predicted == labels).sum()

    print('Accuracy of the model on the %d test samples: %d %%' % (len(test_dataset), 100 * correct / total))

    endTs = time.time()
    print("elapsed time = {} s".format(endTs - startTs))
