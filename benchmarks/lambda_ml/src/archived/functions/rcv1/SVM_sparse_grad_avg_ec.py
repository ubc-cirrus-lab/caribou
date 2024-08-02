import math
import time
import numpy as np

import torch
from data_loader.libsvm_dataset import SparseDatasetWithLines
from archived.s3.get_object import get_object

from archived.elasticache.Memcached import memcached_init
from archived.sync import reduce_batch, clear_bucket

# lambda setting
local_dir = "/tmp"

# algorithm setting
shuffle_dataset = True
random_seed = 42


def handler(event, context):
    try:
        start_time = time.time()
        bucket_name = event['bucket_name']
        worker_index = event['rank']
        num_workers = event['num_workers']
        key = event['file']
        merged_bucket = event['merged_bucket']
        num_features = event['num_features']
        learning_rate = event["learning_rate"]
        batch_size = event["batch_size"]
        num_epochs = event["num_epochs"]
        validation_ratio = event["validation_ratio"]
        elasti_location = event['elasticache']
        endpoint = memcached_init(elasti_location)

        # Reading data from S3
        print(f"Reading training data from bucket = {bucket_name}, key = {key}")
        file = get_object(bucket_name, key).read().decode('utf-8').split("\n")
        print("read data cost {} s".format(time.time() - start_time))

        parse_start = time.time()
        dataset = SparseDatasetWithLines(file, num_features)
        print("parse data cost {} s".format(time.time() - parse_start))

        preprocess_start = time.time()
        dataset_size = len(dataset)
        indices = list(range(dataset_size))
        split = int(np.floor(validation_ratio * dataset_size))
        if shuffle_dataset:
            np.random.seed(random_seed)
            np.random.shuffle(indices)
        train_indices, val_indices = indices[split:], indices[:split]

        train_set = [dataset[i] for i in train_indices]
        val_set = [dataset[i] for i in val_indices]

        print("preprocess data cost {} s".format(time.time() - preprocess_start))
        svm = SparseSVM(train_set, val_set, num_features, num_epochs, learning_rate, batch_size)

        # Training the Model
        for epoch in range(num_epochs):
            epoch_start = time.time()
            num_batches = math.floor(len(train_set) / batch_size)
            print("worker {} epoch {}".format(worker_index, epoch))
            for batch_idx in range(num_batches):
                batch_start = time.time()
                batch_ins, batch_label = svm.next_batch(batch_idx)
                acc = svm.one_epoch(batch_idx, epoch)
                cal_time = time.time() - batch_start

                sync_start = time.time()
                np_w = svm.weights.numpy().flatten()
                postfix = "{}_{}".format(epoch, batch_idx)
                w_merge = reduce_batch(endpoint, np_w, merged_bucket, num_workers, worker_index, postfix)
                svm.weights = torch.from_numpy(w_merge).reshape(num_features, 1)
                sync_time = time.time() - sync_start
                print("computation takes {}s, synchronization cost {}s, batch takes {}s"
                      .format(cal_time, sync_time, time.time() - batch_start))

                if (batch_idx + 1) % 10 == 0:
                    print("Epoch: {}/{}, Step: {}/{}, train acc: {}"
                          .format(epoch + 1, num_epochs, batch_idx + 1, num_batches, acc))

            val_acc = svm.evaluate()
            print("Epoch takes {}s, validation accuracy: {}".format(time.time() - epoch_start, val_acc))

        if worker_index == 0:
            clear_bucket(endpoint)
        print("elapsed time = {} s".format(time.time() - start_time))

    except Exception as e:
        print("Error {}".format(e))
