from __future__ import print_function
import torch.utils.data as data
from PIL import Image
import os
import os.path
# import errno
import numpy as np
import torch
import codecs 
import random
import boto3
import gzip
import time

s3 = boto3.resource('s3')

# Download files from S3 and unzip them
def preprocess_mnist(bucket_name, training_keys, test_keys, local_path):

    if not os.path.exists(os.path.join(local_path, 'raw_data')):
        os.makedirs(os.path.join(local_path, 'raw_data'))

    if not os.path.exists(os.path.join(local_path, 'processed_data')):
        os.makedirs(os.path.join(local_path, 'processed_data'))

    for file in training_keys+test_keys:

        # download .gz file from S3
        s3.Bucket(bucket_name).download_file(file, os.path.join(local_path, 'raw_data', file))

        file_path = os.path.join(local_path, 'raw_data', file)

        # unzip the file and rename it (remove .gz)
        with open(file_path.replace('.gz', ''), 'wb') as out_f, \
                gzip.GzipFile(file_path) as zip_f:
            out_f.write(zip_f.read())
        os.unlink(file_path)

    # for test set, process it and save it back to S3
    test_set = (
        read_image_file(os.path.join(local_path, 'raw_data', test_keys[0].replace('.gz', ''))),
        read_label_file(os.path.join(local_path, 'raw_data', test_keys[1].replace('.gz', '')))
    )

    with open(os.path.join(local_path, 'processed_data', 'test.pt'), 'wb') as f:
        torch.save(test_set, f)

    s3.Bucket(bucket_name).upload_file(os.path.join(local_path, 'processed_data', 'test.pt'), 'test.pt')

    # for training set, return the processed data for following shuffle and partition

    training_x = read_image_file(os.path.join(local_path, 'raw_data', training_keys[0].replace('.gz', '')))
    training_y = read_label_file(os.path.join(local_path, 'raw_data', training_keys[1].replace('.gz', '')))

    return training_x, training_y


def shuffle_and_partition(bucket_name, local_path, training_x, training_y, num_workers):

    # shuffle
    shuffle_start = time.time()
    training_combined = list(zip(training_x, training_y))
    random.shuffle(training_combined)
    x_temp, y_temp = zip(*training_combined)

    num_examples = len(x_temp)
    num_examples_per_worker = num_examples // num_workers
    residue = num_examples % num_workers
    print("shuffle cost {} s".format(time.time()-shuffle_start))

    # split the training set and save 
    split_start = time.time()
    for i in range(num_workers):

        start = (num_examples_per_worker * i) + min(residue, i)
        num_examples_real = num_examples_per_worker + (1 if i < residue else 0)

        training_set = (torch.stack(x_temp[start:start+num_examples_real]), torch.stack(y_temp[start:start+num_examples_real]))

        with open(os.path.join(local_path, 'processed_data', 'training_{}.pt'.format(i)), 'wb') as f:
            torch.save(training_set, f)

        s3.Bucket(bucket_name).upload_file(os.path.join(local_path, 'processed_data', 'training_{}.pt'.format(i)), 'training_{}.pt'.format(i))
    
    print("split cost {} s".format(time.time()-split_start))
    print('Data Partition Done!')


def get_int(b):
    return int(codecs.encode(b, 'hex'), 16)


def read_label_file(path):
    with open(path, 'rb') as f:
        data = f.read()
        assert get_int(data[:4]) == 2049
        length = get_int(data[4:8])
        parsed = np.frombuffer(data, dtype=np.uint8, offset=8)
        return torch.from_numpy(parsed).view(length).long()


def read_image_file(path):
    with open(path, 'rb') as f:
        data = f.read()
        assert get_int(data[:4]) == 2051
        length = get_int(data[4:8])
        num_rows = get_int(data[8:12])
        num_cols = get_int(data[12:16])
        images = []
        parsed = np.frombuffer(data, dtype=np.uint8, offset=16)
        return torch.from_numpy(parsed).view(length, num_rows, num_cols)

# if __name__ == "__main__":
