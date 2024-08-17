import time

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.s3.get_object import get_object
from archived.s3 import clear_bucket
from archived.sync import reduce_epoch, delete_expired_merged_epoch


from archived.old_model import LogisticRegression
from data_loader.libsvm_dataset import DenseDatasetWithLines

# lambda setting
file_bucket = "s3-libsvm"
#model_bucket = "tmp-params"
local_dir = "/tmp"
# merged model format: w_{epoch}
# tmp model format: tmp_w_{epoch}_{worker_id}
# w_prefix = "w_"
# b_prefix = "b_"
# tmp_w_prefix = "tmp_w_"
# tmp_b_prefix = "tmp_b_"

# algorithm setting
num_features = 30
num_classes = 2
learning_rate = 0.01
batch_size = 1000
num_epochs = 2
validation_ratio = .2
shuffle_dataset = True
random_seed = 42


def handler(event, context):
    start_time = time.time()
    bucket = event['bucket_name']
    worker_index = event['rank']
    num_workers = event['num_workers']
    key = event['file']
    tmp_bucket = event['tmp_bucket']
    merged_bucket = event['merged_bucket']
    num_epochs = event['num_epochs']
    learning_rate = event['learning_rate']
    batch_size = event['batch_size']

    print('bucket = {}'.format(bucket))
    print("file = {}".format(key))
    print('tmp bucket = {}'.format(tmp_bucket))
    print('merged bucket = {}'.format(merged_bucket))
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('num epochs = {}'.format(num_epochs))
    print('learning rate = {}'.format(learning_rate))
    print("batch size = {}".format(batch_size))

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

    print("preprocess data cost {} s, dataset size = {}"
          .format(time.time() - preprocess_start, dataset_size))

    model = LogisticRegression(num_features, num_classes)

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # Training the Model
    train_start = time.time()
    for epoch in range(num_epochs):
        epoch_start = time.time()
        epoch_loss = 0
        for batch_index, (items, labels) in enumerate(train_loader):
            # print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
            batch_start = time.time()
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            epoch_loss += loss.data
            loss.backward()

            optimizer.step()

        # Test the Model
        test_start = time.time()
        correct = 0
        total = 0
        test_loss = 0
        for items, labels in validation_loader:
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)
            outputs = model(items)
            test_loss += criterion(outputs, labels).data
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()
        test_time = time.time() - test_start

        print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f, Loss: %.4f, epoch cost %.4f, '
              'batch cost %.4f s: test cost %.4f s, '
              'accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (epoch + 1, num_epochs, batch_index + 1, len(train_indices) / batch_size,
                 time.time() - train_start, epoch_loss.data, time.time() - epoch_start,
                 time.time() - batch_start, test_time,
                 len(val_indices), 100 * correct / total, test_loss / total))

        w = model.linear.weight.data.numpy()
        w_shape = w.shape
        b = model.linear.bias.data.numpy()
        b_shape = b.shape
        w_and_b = np.concatenate((w.flatten(), b.flatten()))
        cal_time = time.time() - epoch_start
        print("Epoch {} calculation cost = {} s".format(epoch, cal_time))

        sync_start = time.time()
        postfix = "{}".format(epoch)
        u_w_b_merge = reduce_epoch(w_and_b, tmp_bucket, merged_bucket, num_workers, worker_index, postfix)

        w_mean = u_w_b_merge[: w_shape[0] * w_shape[1]].reshape(w_shape) / float(num_workers)
        b_mean = u_w_b_merge[w_shape[0] * w_shape[1]:].reshape(b_shape[0]) / float(num_workers)
        model.linear.weight.data = torch.from_numpy(w_mean)
        model.linear.bias.data = torch.from_numpy(b_mean)
        sync_time = time.time() - sync_start
        print("Epoch {} synchronization cost {} s".format(epoch, sync_time))

        if worker_index == 0:
            delete_expired_merged_epoch(merged_bucket, epoch)
        #
        #
        # #file_postfix = "{}_{}".format(epoch, worker_index)
        # if epoch < num_epochs - 1:
        #     if worker_index == 0:
        #         w_merge, b_merge = merge_w_b(model_bucket, num_workers, w.dtype,
        #                                      w.shape, b.shape, tmp_w_prefix, tmp_b_prefix)
        #         put_merged_w_b(model_bucket, w_merge, b_merge,
        #                        str(epoch), w_prefix, b_prefix)
        #         delete_expired_w_b_by_epoch(model_bucket, epoch, tmp_w_prefix, tmp_b_prefix)
        #         model.linear.weight.data = torch.from_numpy(w_merge)
        #         model.linear.bias.data = torch.from_numpy(b_merge)
        #     else:
        #         w_merge, b_merge = get_merged_w_b(model_bucket, str(epoch), w.dtype,
        #                                           w.shape, b.shape, w_prefix, b_prefix)
        #         model.linear.weight.data = torch.from_numpy(w_merge)
        #         model.linear.bias.data = torch.from_numpy(b_merge)

        #print("weight after sync = {}".format(model.linear.weight.data.numpy()[0][:5]))
        #print("bias after sync = {}".format(model.linear.bias.data.numpy()))

        # print("epoch {} synchronization cost {} s".format(epoch, time.time() - sync_start))

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

    if worker_index == 0:
        clear_bucket(merged_bucket)
        clear_bucket(tmp_bucket)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))

