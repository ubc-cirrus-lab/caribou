import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.s3.get_object import get_object
from archived.s3 import clear_bucket
from archived.sync import reduce_epoch, delete_expired_merged_epoch

from archived.old_model import LogisticRegression
from data_loader.libsvm_dataset import DenseDatasetWithLines

# lambda setting
# file_bucket = "s3-libsvm"
# tmp_bucket = "tmp-grads"
# merged_bucket = "merged-params"
local_dir = "/tmp"

# algorithm setting
num_features = 30
num_classes = 2
learning_rate = 0.01
batch_size = 300
num_epochs = 10
num_admm_epochs = 30
validation_ratio = .2
shuffle_dataset = True
random_seed = 42
ep_abs=1e-4
ep_rel=1e-2


def initialize_z_and_u(shape):
    z = np.random.rand(shape[0], shape[1]).astype(np.double)
    u = np.random.rand(shape[0], shape[1]).astype(np.double)
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
    return(stop)


def handler(event, context):
    start_time = time.time()
    bucket = event['bucket_name']
    worker_index = event['rank']
    num_workers = event['num_workers']
    key = event['file']
    tmp_bucket = event['tmp_bucket']
    merged_bucket = event['merged_bucket']
    num_epochs = event['num_epochs']
    num_admm_epochs = event['num_admm_epochs']
    learning_rate = event['learning_rate']
    lam = event['lambda']
    rho = event['rho']
    batch_size = event['batch_size']

    print('bucket = {}'.format(bucket))
    print("file = {}".format(key))
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('tmp bucket = {}'.format(tmp_bucket))
    print('merge bucket = {}'.format(merged_bucket))
    print('num epochs = {}'.format(num_epochs))
    print('num admm epochs = {}'.format(num_admm_epochs))
    print('learning rate = {}'.format(learning_rate))
    print("lambda = {}".format(lam))
    print("rho = {}".format(rho))
    print("batch_size = {}".format(batch_size))

    # read file from s3
    file = get_object(bucket, key).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - start_time))
    # file_path = "../../dataset/agaricus_127d_train.libsvm"
    # file = open(file_path).readlines()

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

    model = LogisticRegression(num_features, num_classes).double()
    print("size of w = {}".format(model.linear.weight.data.size()))

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
    stop = False
    for admm_epoch in range(num_admm_epochs):
        print("ADMM Epoch >>> {}".format(admm_epoch))
        for epoch in range(num_epochs):
            epoch_start = time.time()
            epoch_loss = 0
            for batch_index, (items, labels) in enumerate(train_loader):
                #   print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
                batch_start = time.time()
                items = Variable(items.view(-1, num_features))
                labels = Variable(labels)

                # Forward + Backward + Optimize
                optimizer.zero_grad()
                outputs = model(items.double())
                classify_loss = criterion(outputs, labels)
                epoch_loss += classify_loss.data
                u_z = torch.from_numpy(u).double() - torch.from_numpy(z).double()
                loss = classify_loss
                for name, param in model.named_parameters():
                    if name.split('.')[-1] == "weight":
                        loss += rho / 2.0 * torch.norm(param + u_z, p=2)
                #loss = classify_loss + rho / 2.0 * torch.norm(torch.sum(model.linear.weight, u_z))
                optimizer.zero_grad()
                loss.backward(retain_graph=True)
                optimizer.step()

            # Test the Model
            test_start = time.time()
            correct = 0
            total = 0
            test_loss = 0
            for items, labels in validation_loader:
                items = Variable(items.view(-1, num_features))
                labels = Variable(labels)
                outputs = model(items.double())
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
        u_shape = u.shape

        w_and_b = np.concatenate((w.flatten(), b.flatten()))
        u_w_b = np.concatenate((u.flatten(), w_and_b.flatten()))
        cal_time = time.time() - epoch_start
        print("Epoch {} calculation cost = {} s".format(epoch, cal_time))

        sync_start = time.time()
        postfix = "{}".format(admm_epoch)
        u_w_b_merge = reduce_epoch(u_w_b, tmp_bucket, merged_bucket, num_workers, worker_index, postfix)

        u_mean = u_w_b_merge[:u_shape[0] * u_shape[1]].reshape(u_shape) / float(num_workers)
        w_mean = u_w_b_merge[u_shape[0]*u_shape[1] : u_shape[0]*u_shape[1]+w_shape[0]*w_shape[1]].reshape(w_shape) / float(num_workers)
        b_mean = u_w_b_merge[u_shape[0]*u_shape[1]+w_shape[0]*w_shape[1]:].reshape(b_shape[0]) / float(num_workers)
        #model.linear.weight.data = torch.from_numpy(w)
        model.linear.bias.data = torch.from_numpy(b_mean)
        sync_time = time.time() - sync_start
        print("Epoch {} synchronization cost {} s".format(epoch, sync_time))

        if worker_index == 0:
            delete_expired_merged_epoch(merged_bucket, admm_epoch)

        #z, u, r, s = update_z_u(w, z, u, rho, num_workers, lam)
        #stop = check_stop(ep_abs, ep_rel, r, s, dataset_size, num_features, w, z, u, rho)
        #print("stop = {}".format(stop))

        #z = num_workers * rho / (2 * lam + num_workers * rho) * (w + u_mean)
        z = update_z(w_mean, u_mean, rho, num_workers, lam)
        #print(z)
        u = u + model.linear.weight.data.numpy() - z
        #print(u)

    # Test the Model
    correct = 0
    total = 0
    test_loss = 0
    for items, labels in validation_loader:
        items = Variable(items.view(-1, num_features))
        labels = Variable(labels)
        outputs = model(items.double())
        test_loss += criterion(outputs, labels).data
        _, predicted = torch.max(outputs.data, 1)
        total += labels.size(0)
        correct += (predicted == labels).sum()

    print('Epoch: %d, time = %.4f, accuracy of the model on the %d test samples: %d %%, loss = %f'
          % (epoch, time.time() - train_start, len(val_indices), 100 * correct / total, test_loss / total))

    if worker_index == 0:
        clear_bucket(merged_bucket)
        clear_bucket(tmp_bucket)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
