import time
import numpy as np

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.s3.get_object import get_object
from archived.s3 import clear_bucket
from archived.sync import reduce_epoch, delete_expired_merged_epoch

from archived.old_model.SVM import SVM
from data_loader.YFCCLibsvmDataset import DenseLibsvmDataset

# lambda setting
local_dir = "/tmp"

# algorithm setting
validation_ratio = .1
shuffle_dataset = True
random_seed = 42
ep_abs=1e-4
ep_rel=1e-2


def initialize_z_and_u(shape):
    z = np.random.rand(shape[0], shape[1]).astype(np.float)
    u = np.random.rand(shape[0], shape[1]).astype(np.float)
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
    key = event['file'].split(",")
    tmp_bucket = event['tmp_bucket']
    merged_bucket = event['merged_bucket']
    num_classes = event['num_classes']
    num_features = event['num_features']
    pos_tag = event['pos_tag']
    num_epochs = event['num_epochs']
    num_admm_epochs = event['num_admm_epochs']
    learning_rate = event['learning_rate']
    batch_size = event['batch_size']
    lam = event['lambda']
    rho = event['rho']

    print('bucket = {}'.format(bucket))
    print("file = {}".format(key))
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('tmp bucket = {}'.format(tmp_bucket))
    print('merge bucket = {}'.format(merged_bucket))
    print('num epochs = {}'.format(num_epochs))
    print('num admm epochs = {}'.format(num_admm_epochs))
    print('num classes = {}'.format(num_classes))
    print('num features = {}'.format(num_features))
    print('positive tag = {}'.format(pos_tag))
    print('learning rate = {}'.format(learning_rate))
    print("batch_size = {}".format(batch_size))
    print("lambda = {}".format(lam))
    print("rho = {}".format(rho))

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

    model = SVM(num_features, num_classes).float()
    print("size of w = {}".format(model.linear.weight.data.size()))

    z, u = initialize_z_and_u(model.linear.weight.data.size())
    print("size of z = {}".format(z.shape))
    print("size of u = {}".format(u.shape))

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # Training the Model
    train_start = time.time()
    stop = False
    for admm_epoch in range(num_admm_epochs):
        admm_epoch_start = time.time()
        for epoch in range(num_epochs):
            epoch_start = time.time()
            epoch_loss = 0
            for batch_index, (items, labels) in enumerate(train_loader):
                batch_start = time.time()
                items = Variable(items.view(-1, num_features))
                labels = Variable(labels)

                # Forward + Backward + Optimize
                optimizer.zero_grad()
                outputs = model(items)

                classify_loss = torch.mean(torch.clamp(1 - outputs.t() * labels.float(), min=0))  # hinge loss
                epoch_loss += classify_loss

                u_z = torch.from_numpy(u).float() - torch.from_numpy(z).float()
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
                outputs = model(items)
                test_loss += torch.mean(torch.clamp(1 - outputs.t() * labels.float(), min=0))
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum()
            test_time = time.time() - test_start

            print('ADMM Epoch: [%d/%d], Epoch: [%d/%d], Batch [%d], '
                  'Time: %.4f, Loss: %.4f, epoch cost %.4f, test cost %.4f s, '
                  'accuracy of the model on the %d test samples: %d %%, loss = %f'
                  % (admm_epoch, num_admm_epochs, epoch, num_epochs, batch_index,
                     time.time() - train_start, epoch_loss.data, time.time() - epoch_start, test_time,
                     len(val_indices), 100 * correct / total, test_loss / total))

        w = model.linear.weight.data.numpy()
        w_shape = w.shape
        b = model.linear.bias.data.numpy()
        b_shape = b.shape
        u_shape = u.shape

        w_and_b = np.concatenate((w.flatten(), b.flatten()))
        u_w_b = np.concatenate((u.flatten(), w_and_b.flatten()))
        cal_time = time.time() - epoch_start

        sync_start = time.time()
        postfix = "{}".format(admm_epoch)
        u_w_b_merge = reduce_epoch(u_w_b, tmp_bucket, merged_bucket, num_workers, worker_index, postfix)

        u_mean = u_w_b_merge[:u_shape[0] * u_shape[1]].reshape(u_shape) / float(num_workers)
        w_mean = u_w_b_merge[u_shape[0]*u_shape[1] : u_shape[0]*u_shape[1]+w_shape[0]*w_shape[1]].reshape(w_shape) / float(num_workers)
        b_mean = u_w_b_merge[u_shape[0]*u_shape[1]+w_shape[0]*w_shape[1]:].reshape(b_shape[0]) / float(num_workers)
        #model.linear.weight.data = torch.from_numpy(w)
        model.linear.bias.data = torch.from_numpy(b_mean).float()
        sync_time = time.time() - sync_start

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
        test_start = time.time()
        correct = 0
        total = 0
        test_loss = 0
        for items, labels in validation_loader:
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)
            outputs = model(items)
            test_loss += torch.mean(torch.clamp(1 - outputs.t() * labels.float(), min=0))
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()
        test_time = time.time() - test_start

        print('ADMM Epoch: [%d/%d], Time: %.4f, Loss: %.4f, '
              'ADMM epoch cost %.4f: computation cost %.4f s communication cost %.4f s test cost %.4f s, '
              'accuracy of the model on the %d test samples: %d %%, loss = %f'
              % (admm_epoch, num_admm_epochs, time.time() - train_start, epoch_loss.data,
                 time.time() - admm_epoch_start, cal_time, sync_time, test_time,
                 len(val_indices), 100 * correct / total, test_loss / total))

    # Test the Model
    correct = 0
    total = 0
    test_loss = 0
    for items, labels in validation_loader:
        items = Variable(items.view(-1, num_features))
        labels = Variable(labels)
        outputs = model(items)
        test_loss += torch.mean(torch.clamp(1 - outputs.t() * labels.float(), min=0))
        _, predicted = torch.max(outputs.data, 1)
        total += labels.size(0)
        correct += (predicted == labels).sum()

    print('Time = %.4f, accuracy of the model on the %d test samples: %d %%, loss = %f'
          % (time.time() - train_start, len(val_indices), 100 * correct / total, test_loss / total))

    if worker_index == 0:
       clear_bucket(merged_bucket)
       clear_bucket(tmp_bucket)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
