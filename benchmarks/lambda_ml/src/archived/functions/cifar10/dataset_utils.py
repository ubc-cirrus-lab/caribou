from __future__ import print_function
import numpy as np
import torch
import torch.utils.data as data
import torchvision
import torchvision.transforms as transforms

from PIL import Image
import os
import os.path
import tarfile

import random
import boto3
import time

from data_loader.cifar10_dataset import CIFAR10_subset


s3 = boto3.resource('s3')
base_folder = 'cifar-10-batches-py'
processed_folder = 'processed'


# Download files from S3 and unzip them
def preprocess_cifar10(bucket_name='cifar10dataset', key="cifar-10-python.tar.gz",
                       data_path='/tmp/data', num_workers=8):
    # download zipped file from S3
    print('==> Downloading data from S3..')
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    s3.Bucket(bucket_name).download_file(key, os.path.join(data_path, key))

    # extract file
    cwd = os.getcwd()
    tar = tarfile.open(os.path.join(data_path, key), "r:gz")
    os.chdir(data_path)
    tar.extractall()
    tar.close()
    os.chdir(cwd)
    # delete the zipped file 
    os.remove(os.path.join(data_path, key))

    # Data
    print('==> Preprocessing data..')
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

    # training set
    trainset = torchvision.datasets.CIFAR10(root=data_path, train=True, download=False, transform=transform_train)
    # test set
    testset = torchvision.datasets.CIFAR10(root=data_path, train=False, download=False, transform=transform_test)

    # save test dataset back to S3
    with open(os.path.join(data_path, "test.pt"), 'wb') as f:
        torch.save(testset, f)
    s3.Bucket(bucket_name).upload_file(os.path.join(data_path, "test.pt"), 'test.pt')

    #remove the file after uploading it to S3
    os.remove(os.path.join(data_path, "test.pt"))
    
    if num_workers == 1:
        with open(os.path.join(data_path, "training_0.pt"), 'wb') as f:
            torch.save(trainset, f)
        s3.Bucket(bucket_name).upload_file(os.path.join(data_path, "training_0.pt"), 'training_0.pt')
    else:    
        # shuffle training set
        print('==> Shuffling and partitioning training data..')
        num_examples = len(trainset)
        indices = list(range(num_examples))
        random.shuffle(indices)
    
        num_examples_per_worker = num_examples // num_workers
        residue = num_examples % num_workers
    
        for i in range(num_workers):
            start = (num_examples_per_worker * i) + min(residue, i)
            num_examples_real = num_examples_per_worker + (1 if i < residue else 0)
    
            # print("first 10 labels of original[{}]:{}".format(i, [trainset.train_labels[i] for i in indices[start:start+num_examples_real]][0:10]))
            training_subset = CIFAR10_subset(train=True,
                                             train_data=[trainset.train_data[i] for i in indices[start:start+num_examples_real]],
                                             train_labels=[trainset.train_labels[i] for i in indices[start:start+num_examples_real]],
                                             test_data=None, test_labels=None, transform=transform_train)
            # print("first 10 labels of subset[{}]:{}".format(i, training_subset.train_labels[0:10]))
    
            # save training subset back to S3
            with open(os.path.join(data_path, 'training_{}.pt'.format(i)), 'wb') as f:
                torch.save(training_subset, f)
            # subset_load = torch.load(os.path.join(data_path, 'training_{}.pt'.format(i)))
            # print("first 10 labels of subset_load[{}]:{}".format(i, subset_load.train_labels[0:10]))

            s3.Bucket(bucket_name).upload_file(os.path.join(data_path, 'training_{}.pt'.format(i)), 'training_{}.pt'.format(i))
            os.remove(os.path.join(data_path, 'training_{}.pt'.format(i)))


# if __name__ == "__main__":
#     preprocess_cifar10(bucket_name = ' ', key = "cifar-10-python.tar.gz", data_path = '/home/pytorch-cifar/data/', num_workers = 4)
