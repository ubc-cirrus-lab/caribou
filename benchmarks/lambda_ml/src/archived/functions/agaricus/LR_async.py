import time
import urllib.parse
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.s3.get_object import get_object, get_object_or_wait
from archived.s3 import put_object
from archived.sync import clear_bucket

from archived.old_model import LogisticRegression
from data_loader.libsvm_dataset import DenseDatasetWithLines
from archived.sync import SyncMeta

# lambda setting
# grad_bucket = "tmp-grads"
model_bucket = "tmp-updates"
local_dir = "/tmp"
w_prefix = "w_"
b_prefix = "b_"

# algorithm setting
num_features = 150
num_classes = 2
learning_rate = 0.1
batch_size = 100
num_epochs = 2
validation_ratio = .2
shuffle_dataset = True
random_seed = 42


def handler(event, context):
    start_time = time.time()
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print('bucket = {}'.format(bucket))
    print('key = {}'.format(key))

    key_splits = key.split("_")
    worker_index = int(key_splits[0])
    num_worker = int(key_splits[1])
    sync_meta = SyncMeta(worker_index, num_worker)
    print("synchronization meta {}".format(sync_meta.__str__()))

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

    print("preprocess data cost {} s".format(time.time() - preprocess_start))

    model = LogisticRegression(num_features, num_classes)

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # Training the Model
    for epoch in range(num_epochs):
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

            print("forward and backward cost {} s".format(time.time()-batch_start))

            optimizer.step()

            w = model.linear.weight.data.numpy()
            b = model.linear.bias.data.numpy()

            #print("dtype of grad = {}".format(w_grad.dtype))
            print("weight before update = {}".format(w[0][0:5]))
            print("bias before update = {}".format(b))

            sync_start = time.time()
            put_object(model_bucket, w_prefix, w.tobytes())
            put_object(model_bucket, b_prefix, b.tobytes())

            w_data = get_object_or_wait(model_bucket, w_prefix, 0.1).read()
            w_new = np.frombuffer(w_data, dtype=w.dtype).reshape(w.shape)
            b_data = get_object_or_wait(model_bucket, b_prefix, 0.1).read()
            b_new = np.frombuffer(b_data, dtype=b.dtype).reshape(b.shape)

            model.linear.weight.data = torch.from_numpy(w_new)
            model.linear.bias.data = torch.from_numpy(b_new)

            print("weight after update = {}".format(model.linear.weight.data.numpy()[0][:5]))
            print("b_grad after update = {}".format(model.linear.bias.data.numpy()))

            print("synchronization cost {} s".format(time.time() - sync_start))

            print("batch cost {} s".format(time.time() - batch_start))

            if (batch_index + 1) % 10 == 0:
                print('Epoch: [%d/%d], Step: [%d/%d], Loss: %.4f'
                      % (epoch + 1, num_epochs, batch_index + 1, len(train_indices) / batch_size, loss.data))

    if worker_index == 0:
        clear_bucket(model_bucket)

    # Test the Model
    correct = 0
    total = 0
    for items, labels in validation_loader:
        items = Variable(items.view(-1, num_features))
        # items = Variable(items)
        outputs = model(items)
        _, predicted = torch.max(outputs.data, 1)
        total += labels.size(0)
        correct += (predicted == labels).sum()

    print('Accuracy of the model on the %d test samples: %d %%' % (len(val_indices), 100 * correct / total))

    end_time = time.time()
    print("elapsed time = {} s".format(end_time - start_time))
