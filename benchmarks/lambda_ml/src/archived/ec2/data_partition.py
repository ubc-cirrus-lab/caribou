import time
import os

import random
import numpy as np

import torch
from torchvision import datasets, transforms
print(dir(transforms))
import torch.distributed as dist
from torch.utils.data import DataLoader
from torch.utils.data.sampler import SubsetRandomSampler

from data_loader.libsvm_dataset import DenseDatasetWithLines, DenseDatasetWithNP
from data_loader.YFCCLibsvmDataset import DenseLibsvmDataset
from data_loader.libsvm_dataset import SparseDatasetWithLines
from data_loader.cifar10_dataset import CIFAR10_subset


def dist_is_initialized():
    if dist.is_available():
        if dist.is_initialized():
            return True
    return False


class Partition(object):
    """ Dataset-like object, but only access a subset of it. """

    def __init__(self, data, index):
        self.data = data
        self.index = index

    def __len__(self):
        return len(self.index)

    def __getitem__(self, index):
        data_idx = self.index[index]
        return self.data[data_idx]


class DataPartitioner(object):
    """ Partitions a dataset into different chuncks. """

    def __init__(self, data, sizes=[0.7, 0.2, 0.1], seed=1234):
        self.data = data
        self.partitions = []
        rng = random.Random()
        rng.seed(seed)
        data_len = len(data)
        indexes = [x for x in range(0, data_len)]
        rng.shuffle(indexes)

        for frac in sizes:
            part_len = int(frac * data_len)
            self.partitions.append(indexes[0:part_len])
            indexes = indexes[part_len:]

    def use(self, partition):
        return Partition(self.data, self.partitions[partition])


def partition_mnist(batch_size, path, download=True):
    """ Partitioning MNIST """
    train_dataset = datasets.MNIST(path, train=True, download=download,
                                   transform=transforms.Compose([transforms.ToTensor(),
                                                                 transforms.Normalize((0.1307, ), (0.3081, ))]))
    test_dataset = datasets.MNIST(path, train=False, download=download,
                                   transform=transforms.Compose([transforms.ToTensor(),
                                                                 transforms.Normalize((0.1307,), (0.3081,))]))
    size = dist.get_world_size()
    bsz = int(batch_size / float(size))
    train_partition_sizes = [1.0 / size for _ in range(size)]
    train_partition = DataPartitioner(train_dataset, train_partition_sizes)
    train_partition = train_partition.use(dist.get_rank())
    train_loader = DataLoader(
        train_partition, batch_size=bsz, shuffle=True)
    test_loader = DataLoader(
        test_dataset, batch_size=batch_size, shuffle=True)
    return train_loader, bsz, test_loader


def partition_cifar10(batch_size, path, args, download=True):
    """ Partitioning Cifar10 """
    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])

    transform_test = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])

    train_dataset = datasets.CIFAR10(path, train=True, download=download, transform=transform_train)
    test_dataset = datasets.CIFAR10(path, train=False, download=download, transform=transform_test)
    size = 1
    rank = 0
    if dist_is_initialized():
        size = dist.get_world_size()
        rank = dist.get_rank()
    else:
        size = args.world_size
        rank = args.rank
    bsz = int(batch_size / float(size))
    train_partition_sizes = [1.0 / size for _ in range(size)]
    train_partition = DataPartitioner(train_dataset, train_partition_sizes)
    train_partition = train_partition.use(rank)
    train_loader = DataLoader(
        train_partition, batch_size=bsz, shuffle=True)
    test_loader = DataLoader(
        test_dataset, batch_size=batch_size, shuffle=True)
    return train_loader, bsz, test_loader


def generate_cifar10_fl(path, imbalance_ratio, download=True):
    """ Partitioning Cifar10 """
    n_classes = 10
    # transform_train = transforms.Compose([
    #     transforms.RandomCrop(32, padding=4),
    #     transforms.RandomHorizontalFlip(),
    #     transforms.ToTensor(),
    #     transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    #     transforms.Grayscale(num_output_channels=1)
    # ])
    transform_train = transforms.Grayscale(num_output_channels=1)

    train_dataset = datasets.CIFAR10(path, train=True, download=download, transform=transform_train)

    print(train_dataset.train_data.shape)
    print(len(train_dataset.train_labels))

    np.random.seed(123)

    fl_train_data = []
    fl_train_label = []

    # row: worker, col: label
    count = np.zeros((10, 10))

    # data for 10 workers
    for i in range(n_classes):
        fl_train_data.append([])
        fl_train_label.append([])

    # partition train dataset
    for i in range(len(train_dataset.train_labels)):
        data = train_dataset.train_data[i, :, :, :]
        label = train_dataset.train_labels[i]
        np_rand = np.random.uniform(0, 1)
        is_other = True if np_rand > imbalance_ratio else False
        if is_other:
            w_id = random.randint(0, n_classes-1)
            fl_train_data[w_id].append(data)
            fl_train_label[w_id].append(label)
            count[w_id, label] += 1
        else:
            fl_train_data[label].append(data)
            fl_train_label[label].append(label)
            count[label, label] += 1

    for i in range(n_classes):
        data_file_name = "{}_{}_fl_data".format(i, n_classes)
        label_file_name = "{}_{}_fl_label".format(i, n_classes)
        train_data = np.stack(fl_train_data[i], axis=0)
        print("worker {} data shape = {}".format(i, train_data.shape))
        train_label = np.array(fl_train_label[i])
        print("worker {} label shape = {}".format(i, train_label.shape))
        #np.save(os.path.join(path, data_file_name), train_data)
        #np.save(os.path.join(path, label_file_name), train_label)

    for i in range(n_classes):
        print("data distribution of worker {}: {}".format(i, count[i, :]))


def load_cifar10_fl(root_path, features_path, labels_path, batch_size):
    """ Load Cifar10 """

    start_time = time.time()
    features_matrix = np.load(features_path)
    print("read features matrix cost {} s".format(time.time() - start_time))
    print("feature matrix shape = {}, dtype = {}".format(features_matrix.shape, features_matrix.dtype))
    print("feature matrix sample = {}".format(features_matrix[0]))
    row_features = features_matrix.shape[0]
    col_features = features_matrix.shape[1]

    labels_matrix = np.load(labels_path)
    print("read label matrix cost {} s".format(time.time() - start_time))
    print("label matrix shape = {}, dtype = {}".format(labels_matrix.shape, labels_matrix.dtype))
    print("label matrix sample = {}".format(labels_matrix[0:10]))
    row_labels = labels_matrix.shape[0]

    if row_features != row_labels:
        raise AssertionError("row of feature matrix is {}, but row of label matrix is {}."
                             .format(row_features, row_labels))

    parse_start = time.time()

    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()

    train_dataset = CIFAR10_subset(True, list(features_matrix), list(labels_matrix), None, None)
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

    test_dataset = datasets.CIFAR10(root_path, train=False, download=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=True)

    train_dataset_size = len(train_dataset)
    test_dataset_size = len(test_dataset)

    print("preprocess data cost {} s, train dataset size = {}, test dataset size = {}"
          .format(time.time() - preprocess_start, train_dataset_size, test_dataset_size))

    return train_loader, test_loader


def partition_vgg16_cifar100_fc(batch_size, features_path, labels_path, validation_ratio, shuffle=True):
    """ Partitioning fully-connected layer of vgg16 on cifar100"""

    start_time = time.time()
    features_matrix = np.load(features_path)
    print("read features matrix cost {} s".format(time.time() - start_time))
    print("feature matrix shape = {}, dtype = {}".format(features_matrix.shape, features_matrix.dtype))
    print("feature matrix sample = {}".format(features_matrix[0]))
    row_features = features_matrix.shape[0]
    col_features = features_matrix.shape[1]

    labels_matrix = np.load(labels_path)
    print("read label matrix cost {} s".format(time.time() - start_time))
    print("label matrix shape = {}, dtype = {}".format(labels_matrix.shape, labels_matrix.dtype))
    print("label matrix sample = {}".format(labels_matrix[0:10]))
    row_labels = labels_matrix.shape[0]

    if row_features != row_labels:
        raise AssertionError("row of feature matrix is {}, but row of label matrix is {}."
                             .format(row_features, row_labels))

    parse_start = time.time()
    dataset = DenseDatasetWithNP(col_features, features_matrix, labels_matrix)
    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()
    # Creating data indices for training and validation splits:
    dataset_size = len(dataset)

    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    if shuffle:
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating PT data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)

    train_loader = DataLoader(dataset, batch_size=batch_size, sampler=train_sampler)
    validation_loader = DataLoader(dataset, batch_size=batch_size, sampler=valid_sampler)

    print("preprocess data cost {} s, dataset size = {}"
          .format(time.time() - preprocess_start, dataset_size))

    return train_loader, validation_loader


def partition_higgs(batch_size, file_name, validation_ratio):
    parse_start = time.time()
    f = open(file_name).readlines()
    dataset = DenseDatasetWithLines(f, 30)
    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    random_seed = 42
    np.random.seed(random_seed)
    np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating PT data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)

    train_loader = torch.utils.data.DataLoader(dataset,
                                               batch_size=batch_size,
                                               sampler=train_sampler)
    test_loader = torch.utils.data.DataLoader(dataset,
                                              batch_size=batch_size,
                                              sampler=valid_sampler)

    print("preprocess data cost {} s".format(time.time() - preprocess_start))
    return train_loader, test_loader


def partition_yfcc100m(file_list, n_features, pos_tag, batch_size, validation_ratio):
    parse_start = time.time()
    f = open(file_list[0]).readlines()
    dataset = DenseLibsvmDataset(f, n_features, pos_tag)
    if len(file_list) > 1:
        for file_name in file_list[1:]:
            f = open(file_name).readlines()
            dataset.add_more(f)

    total_count = dataset.__len__()
    pos_count = 0
    for i in range(total_count):
        if dataset.__getitem__(i)[1] == 1:
            pos_count += 1
    print("{} positive observations out of {}".format(pos_count, total_count))

    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    random_seed = 42
    np.random.seed(random_seed)
    np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating PT data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)

    train_loader = torch.utils.data.DataLoader(dataset,
                                               batch_size=batch_size,
                                               sampler=train_sampler)
    test_loader = torch.utils.data.DataLoader(dataset,
                                              batch_size=batch_size,
                                              sampler=valid_sampler)

    print("preprocess data cost {} s".format(time.time() - preprocess_start))
    return train_loader, test_loader


def partition_agaricus(batch_size, train_file, test_file):
    train_dataset = SparseDatasetWithLines(train_file, 127)
    test_dataset = SparseDatasetWithLines(test_file, 127)

    size = dist.get_world_size()
    bsz = 1 if batch_size == 1 else int(batch_size / float(size))
    train_partition_sizes = [1.0 / size for _ in range(size)]
    train_partition = DataPartitioner(train_dataset, train_partition_sizes)
    train_partition = train_partition.use(dist.get_rank())
    train_loader = DataLoader(
        train_partition, batch_size=bsz, shuffle=True)
    test_loader = DataLoader(
        test_dataset, batch_size=batch_size, shuffle=True)
    return train_partition, train_loader, bsz, test_loader


def partition_sparse(file, num_feature):
    train_dataset = SparseDatasetWithLines(file, num_feature)
    size = 1
    rank = 0
    if dist_is_initialized():
        size = dist.get_world_size()
        rank = dist.get_rank()
    train_partition_sizes = [1.0 / size for _ in range(size)]
    train_partition = DataPartitioner(train_dataset, train_partition_sizes)
    train_partition = train_partition.use(rank)
    return train_partition


if __name__ == '__main__':
    path = "D:\\Downloads\\datasets\\cifar10\\"
    generate_cifar10_fl(path, 0.8)
    # feature_file = path + "0_10_fl_data.npy"
    # label_file = path + "0_10_fl_label.npy"
    # load_cifar10_fl(path, feature_file, label_file, 500)
