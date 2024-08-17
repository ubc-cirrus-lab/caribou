import math
import urllib.parse
import torch

from data_loader.libsvm_dataset import SparseDatasetWithLines
from archived.s3.get_object import *

# lambda setting
grad_bucket = "sparse-grads"
model_bucket = "sparse-updates"
local_dir = "/tmp"
w_prefix = "w_"

shuffle_dataset = True
random_seed = 42


def handler(event, context):
    try:
        start_time = time.time()
        num_features = event['num_features']
        learning_rate = event["learning_rate"]
        batch_size = event["batch_size"]
        num_epochs = event["num_epochs"]
        validation_ratio = event["validation_ratio"]

        # Reading data from S3
        bucket_name = event['bucket_name']
        key = urllib.parse.unquote_plus(event['key'], encoding='utf-8')
        print("Reading training data from bucket = {}, key = {}"
              .format(bucket_name, key))
        key_splits = key.split("_")
        worker_index = int(key_splits[0])
        num_worker = int(key_splits[1])

        # read file from s3
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
            print(f"worker {worker_index} epoch {epoch}")
            for batch_idx in range(num_batches):
                batch_start = time.time()
                batch_ins, batch_label = svm.next_batch(batch_idx)
                acc = svm.one_epoch(batch_idx, epoch)
            cal_time = time.time() - epoch_start

            sync_start = time.time()
            np_grad = svm.weights.numpy().flatten()
            put_object(grad_bucket, w_prefix + str(worker_index), np_grad.tobytes())
            file_postfix = str(epoch)
            if worker_index == 0:
                w_grad_merge = merge_weights(grad_bucket, num_worker, np_grad.dtype, np_grad.shape)
                put_object(model_bucket, w_prefix + file_postfix, w_grad_merge.tobytes())
                # delete_expired_w_b(model_bucket, epoch, batch_idx, w_grad_prefix)
                svm.weights = torch.from_numpy(w_grad_merge).reshape(num_features, 1)
            else:
                w_data = get_object_or_wait(model_bucket, w_prefix + file_postfix, 0.1).read()
                w_grad_merge = np.frombuffer(w_data, dtype=np_grad.dtype).reshape(np_grad.shape)
                svm.weights = torch.from_numpy(w_grad_merge).reshape(num_features, 1)
            sync_time = time.time() - sync_start
            print(f"synchronization cost {time.time() - sync_start}s")

            test_start = time.time()
            val_acc = svm.evaluate()
            test_time = time.time() - test_start

            print("Epoch: {}/{}, Step: {}/{}, epoch cost {}s, "
                  "cal cost {}s, sync cost {}s, test cost %.4f s, test accuracy: {}"
                  .format(epoch + 1, num_epochs, batch_idx + 1, num_batches, time.time() - epoch_start,
                          cal_time, sync_time, test_time, val_acc))

        if worker_index == 0:
            clear_bucket(model_bucket)
            clear_bucket(grad_bucket)
        print("elapsed time = {} s".format(time.time() - start_time))

    except Exception as e:
        print("Error {}".format(e))
