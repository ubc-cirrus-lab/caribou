import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from data_loader import libsvm_dataset

from utils.constants import Prefix, MLModel, Optimization, Synchronization
from src.storage.s3.s3_type import S3Storage
from communicator import S3Communicator

from src.storage import DynamoTable
from src.storage.dynamo import dynamo_operator

from model import linear_models


def initialize_z_and_u(shape):
    z = np.random.rand(shape[0], shape[1]).astype(np.float32)
    u = np.random.rand(shape[0], shape[1]).astype(np.float32)
    return z, u


def update_z_u(w, z, u, rho, n, lam_0):
    z_new = w + u
    z_tem = abs(z_new) - lam_0 / float(n * rho)
    z_new = np.sign(z_new) * z_tem * (z_tem > 0)

    s = z_new - z
    r = w - np.ones(w.shape[0] * w.shape[1]).astype(np.float).reshape(w.shape) * z_new
    u_new = u + r
    return z_new, s, r, s


def update_z(w, u, rho, n, lam_0):
    z_new = w + u
    z_tem = abs(z_new) - lam_0 / float(n * rho)
    z_new = np.sign(z_new) * z_tem * (z_tem > 0)
    return z_new


def check_stop(ep_abs, ep_rel, r, s, n, p, w, z, u, rho):
    e_pri = (n*p)**(0.5) * ep_abs + ep_rel * (max(np.sum(w**2),np.sum(n*z**2)))**(0.5)
    e_dual = (p)**(0.5) * ep_abs + ep_rel * rho * (np.sum(u**2))**(0.5)/(n)**(0.5)
    print("r^2 = {}, s^2 = {}, e_pri = {}, e_dual = {}".
          format(np.sum(r**2), e_pri, np.sum(s**2), e_dual))
    stop = (np.sum(r**2) <= e_pri**2) & (np.sum(s**2) <= e_dual**2)
    return stop


def handler(event, context):
    start_time = time.time()

    # dataset setting
    file = event['file']
    data_bucket = event['data_bucket']
    dataset_type = event['dataset_type']
    n_features = event['n_features']
    n_classes = event['n_classes']
    n_workers = event['n_workers']
    worker_index = event['worker_index']
    tmp_bucket = event['tmp_bucket']
    merged_bucket = event['merged_bucket']

    # training setting
    model_name = event['model']
    optim = event['optim']
    sync_mode = event['sync_mode']
    assert model_name.lower() in MLModel.Linear_Models
    assert optim.lower() == Optimization.ADMM
    assert sync_mode.lower() in [Synchronization.Reduce, Synchronization.Reduce_Scatter]

    # hyper-parameter
    learning_rate = event['lr']
    batch_size = event['batch_size']
    n_epochs = event['n_epochs']
    valid_ratio = event['valid_ratio']
    n_admm_epochs = event['n_admm_epochs']
    lam = event['lambda']
    rho = event['rho']

    print('data bucket = {}'.format(data_bucket))
    print("file = {}".format(file))
    print('number of workers = {}'.format(n_workers))
    print('worker index = {}'.format(worker_index))
    print('model = {}'.format(model_name))
    print('optimization = {}'.format(optim))
    print('sync mode = {}'.format(sync_mode))

    storage = S3Storage()
    communicator = S3Communicator(storage, tmp_bucket, merged_bucket, n_workers, worker_index)

    # Read file from s3
    read_start = time.time()
    lines = storage.load(file, data_bucket).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - read_start))

    parse_start = time.time()
    dataset = libsvm_dataset.from_lines(lines, n_features, dataset_type)
    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()
    # Creating data indices for training and validation splits:
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(valid_ratio * dataset_size))
    shuffle_dataset = True
    random_seed = 100
    if shuffle_dataset:
        np.random.seed(random_seed)
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)
    train_loader = torch.utils.data.DataLoader(dataset,
                                               batch_size=batch_size,
                                               sampler=train_sampler)
    n_train_batch = len(train_loader)
    validation_loader = torch.utils.data.DataLoader(dataset,
                                                    batch_size=batch_size,
                                                    sampler=valid_sampler)
    print("preprocess data cost {} s, dataset size = {}"
          .format(time.time() - preprocess_start, dataset_size))

    model = linear_models.get_model(model_name, n_features, n_classes)

    z, u = initialize_z_and_u(model.linear.weight.data.size())
    print("size of z = {}".format(z.shape))
    print("size of u = {}".format(u.shape))

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # Training the Model
    train_start = time.time()
    for admm_epoch in range(n_admm_epochs):
        print(">>> ADMM Epoch[{}]".format(admm_epoch))
        admm_epoch_start = time.time()
        admm_epoch_cal_time = 0
        admm_epoch_sync_time = 0
        admm_epoch_test_time = 0
        for epoch in range(n_epochs):
            epoch_start = time.time()
            epoch_loss = 0.
            for batch_index, (items, labels) in enumerate(train_loader):
                batch_start = time.time()
                items = Variable(items.view(-1, n_features))
                labels = Variable(labels)

                # Forward + Backward + Optimize
                optimizer.zero_grad()
                outputs = model(items)
                classify_loss = criterion(outputs, labels)
                epoch_loss += classify_loss.data
                u_z = torch.from_numpy(u) - torch.from_numpy(z)
                loss = classify_loss
                for name, param in model.named_parameters():
                    if name.split('.')[-1] == "weight":
                        loss += rho / 2.0 * torch.norm(param + u_z, p=2)
                        # loss = classify_loss + rho / 2.0 * torch.norm(torch.sum(model.linear.weight, u_z))
                optimizer.zero_grad()
                loss.backward(retain_graph=True)
                optimizer.step()

            epoch_cal_time = time.time() - epoch_start
            admm_epoch_cal_time += epoch_cal_time

            # Test the Model
            test_start = time.time()
            n_test_correct = 0
            n_test = 0
            test_loss = 0
            for items, labels in validation_loader:
                items = Variable(items.view(-1, n_features))
                labels = Variable(labels)
                outputs = model(items)
                test_loss += criterion(outputs, labels).data
                _, predicted = torch.max(outputs.data, 1)
                n_test += labels.size(0)
                n_test_correct += (predicted == labels).sum()
            epoch_test_time = time.time() - test_start
            admm_epoch_test_time += epoch_test_time

            print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                  'cal cost %.4f s, test cost %.4f s, accuracy of the model on the %d test samples: %d %%, loss = %f'
                  % (epoch + 1, n_epochs, batch_index + 1, n_train_batch,
                     time.time() - train_start, epoch_loss, time.time() - epoch_start,
                     epoch_cal_time, epoch_test_time,
                     n_test, 100. * n_test_correct / n_test, test_loss / n_test))

        sync_start = time.time()
        w = model.linear.weight.data.numpy()
        w_shape = w.shape
        b = model.linear.bias.data.numpy()
        b_shape = b.shape
        u_shape = u.shape

        w_b = np.concatenate((w.flatten(), b.flatten()))
        u_w_b = np.concatenate((u.flatten(), w_b.flatten()))

        postfix = "{}".format(admm_epoch)

        # admm does not support async
        if sync_mode == "reduce":
            u_w_b_merge = communicator.reduce_epoch(u_w_b, postfix)
        elif sync_mode == "reduce_scatter":
            u_w_b_merge = communicator.reduce_scatter_epoch(u_w_b, postfix)

        u_mean = u_w_b_merge[:u_shape[0] * u_shape[1]].reshape(u_shape) / float(n_workers)
        w_mean = u_w_b_merge[u_shape[0] * u_shape[1]: u_shape[0] * u_shape[1] + w_shape[0] * w_shape[1]]\
                     .reshape(w_shape) / float(n_workers)
        b_mean = u_w_b_merge[u_shape[0] * u_shape[1] + w_shape[0] * w_shape[1]:]\
                     .reshape(b_shape[0]) / float(n_workers)

        model.linear.weight.data = torch.from_numpy(w_mean)
        model.linear.bias.data = torch.from_numpy(b_mean)
        admm_epoch_sync_time += time.time() - sync_start

        if worker_index == 0:
            delete_start = time.time()
            communicator.delete_expired_epoch(admm_epoch)
            admm_epoch_sync_time += time.time() - delete_start

        # z, u, r, s = update_z_u(w, z, u, rho, num_workers, lam)
        # stop = check_stop(ep_abs, ep_rel, r, s, dataset_size, num_features, w, z, u, rho)
        # print("stop = {}".format(stop))

        # z = num_workers * rho / (2 * lam + num_workers * rho) * (w + u_mean)
        z = update_z(w_mean, u_mean, rho, n_workers, lam)
        u = u + model.linear.weight.data.numpy() - z

        print("ADMM Epoch[{}] Epoch[{}] finishes, cost {} s, cal cost {} s, sync cost {} s, test cost {} s"
              .format(admm_epoch, epoch, time.time() - admm_epoch_start,
                      admm_epoch_cal_time, admm_epoch_sync_time, admm_epoch_test_time))

    # Test the Model
    n_test_correct = 0
    n_test = 0
    test_loss = 0
    for items, labels in validation_loader:
        items = Variable(items.view(-1, n_features))
        labels = Variable(labels)
        outputs = model(items)
        test_loss += criterion(outputs, labels).data
        _, predicted = torch.max(outputs.data, 1)
        n_test += labels.size(0)
        n_test_correct += (predicted == labels).sum()

    test_loss /= n_test
    test_acc = n_test_correct / float(n_test)
    print('Train finish, time = %.4f, accuracy of the model on the %d test samples: %d %%, loss = %f'
          % (time.time() - train_start, n_test, 100. * test_acc, test_loss))

    if worker_index == 0:
        storage.clear(tmp_bucket)
        storage.clear(merged_bucket)
        # write record to dynamodb table
        recorder_table_name = "recoder"
        dynamo_client = dynamo_operator.get_client()
        recorder_tb = DynamoTable(dynamo_client, recorder_table_name)
        recorder_tb.save("{},{}".format(test_loss, test_acc), learning_rate, "learning_rate")

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
