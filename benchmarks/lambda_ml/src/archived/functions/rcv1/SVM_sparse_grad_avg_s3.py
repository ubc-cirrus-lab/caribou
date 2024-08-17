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
w_grad_prefix = "w_grad_"

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
        print(f"Reading training data from bucket = {bucket_name}, key = {key}")
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

                np_grad = svm.weights.numpy().flatten()
                print(f"computation takes {time.time() - batch_start}s")

                sync_start = time.time()
                put_object(grad_bucket, w_grad_prefix + str(worker_index), np_grad.tobytes())

                file_postfix = "{}_{}".format(epoch, batch_idx)
                if worker_index == 0:
                    w_grad_merge = merge_weights(grad_bucket, num_worker, np_grad.dtype, np_grad.shape)
                    put_object(model_bucket, w_grad_prefix + file_postfix, w_grad_merge.tobytes())
                    # delete_expired_w_b(model_bucket, epoch, batch_idx, w_grad_prefix)
                    svm.weights = torch.from_numpy(w_grad_merge).reshape(num_features, 1)
                else:
                    w_data = get_object_or_wait(model_bucket, w_grad_prefix + file_postfix, 0.1).read()
                    w_grad_merge = np.frombuffer(w_data, dtype=np_grad.dtype).reshape(np_grad.shape)
                    svm.weights = torch.from_numpy(w_grad_merge).reshape(num_features, 1)
                print(f"synchronization cost {time.time() - sync_start}s")
                print(f"batch takes {time.time() - batch_start}s")

                if (batch_idx + 1) % 10 == 0:
                    print(f"Epoch: {epoch + 1}/{num_epochs}, Step: {batch_idx + 1}/{len(train_indices) / batch_size}, "
                          f"train acc: {acc}")

            val_acc = svm.evaluate()
            print(f"validation accuracy: {val_acc}")
            print(f"Epoch takes {time.time() - epoch_start}s")

        if worker_index == 0:
            clear_bucket(model_bucket)
            clear_bucket(grad_bucket)
        print("elapsed time = {} s".format(time.time() - start_time))

    except Exception as e:
        print("Error {}".format(e))