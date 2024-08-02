import urllib.parse
import numpy as np
import time

import torch

from data_loader import libsvm_dataset

from utils.constants import Prefix, Synchronization
from src.storage.s3.s3_type import S3Storage
from communicator import S3Communicator

from model import cluster_models
from model.cluster_models import KMeans, SparseKMeans


def sparse_centroid_to_numpy(centroid_sparse_tensor, nr_cluster):
    cent_lst = [centroid_sparse_tensor[i].to_dense().numpy() for i in range(nr_cluster)]
    centroid = np.array(cent_lst)
    return centroid


def centroid_bytes2np(centroid_bytes, n_cluster, data_type, with_error=False):
    centroid_np = np.frombuffer(centroid_bytes, dtype=data_type)
    if with_error:
        centroid_size = centroid_np.shape[0] - 1
        return centroid_np[-1], centroid_np[0:-1].reshape(n_cluster, int(centroid_size / n_cluster))
    else:
        centroid_size = centroid_np.shape[0]
        return centroid_np.reshape(n_cluster, int(centroid_size / n_cluster))


def new_centroids_with_error(model, dataset, dataset_type, old_centroids, epoch, n_features, n_clusters, data_type):
    compute_start = time.time()
    if dataset_type == "dense_libsvm":
        model = KMeans(dataset, old_centroids)
    elif dataset_type == "sparse_libsvm":
        model = SparseKMeans(dataset, old_centroids, n_features, n_clusters)
    model.find_nearest_cluster()
    new_centroids = model.get_centroids("numpy").reshape(-1)

    compute_end = time.time()
    print("Epoch = {}, compute new centroids time: {}, error = {}"
          .format(epoch, compute_end - compute_start, model.error))
    res = np.append(new_centroids, model.error).astype(data_type)
    return res


def compute_average_centroids(storage, avg_cent_bucket, worker_cent_bucket, n_workers, shape, epoch, data_type):
    assert isinstance(storage, S3Storage)

    n_files = 0
    centroids_vec_list = []
    error_list = []
    while n_files < n_workers:
        n_files = 0
        centroids_vec_list = []
        error_list = []
        objects = storage.list(worker_cent_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                cent_bytes = storage.load(file_key, worker_cent_bucket).read()
                cent_with_error = np.frombuffer(cent_bytes, dtype=data_type)
                cent_np = cent_with_error[0:-1].reshape(shape)
                error = cent_with_error[-1]
                centroids_vec_list.append(cent_np)
                error_list.append(error)
                n_files = n_files + 1
        else:
            print('No objects in {}'.format(worker_cent_bucket))

    avg_cent = np.average(np.array(centroids_vec_list), axis=0).reshape(-1)
    avg_error = np.mean(np.array(error_list))
    storage.clear(worker_cent_bucket)

    print("Average error for {}-th epoch: {}".format(epoch, avg_error))
    res = np.append(avg_cent, avg_error).astype(data_type)
    storage.save(res.tobytes(), f"avg-{epoch}", avg_cent_bucket)
    return True


def handler(event, context):
    # dataset
    data_bucket = event['data_bucket']
    file = event['file']
    dataset_type = event["dataset_type"]
    n_features = event['n_features']
    tmp_bucket = event["tmp_bucket"]
    merged_bucket = event["merged_bucket"]
    assert dataset_type == "sparse_libsvm"

    # hyper-parameter
    n_clusters = event['n_clusters']
    n_epochs = event["n_epochs"]
    threshold = event["threshold"]
    sync_mode = event["sync_mode"]
    n_workers = event["n_workers"]
    worker_index = event['worker_index']
    assert sync_mode.lower() in [Synchronization.Reduce, Synchronization.Reduce_Scatter]

    print('data bucket = {}'.format(data_bucket))
    print("file = {}".format(file))
    print('number of workers = {}'.format(n_workers))
    print('worker index = {}'.format(worker_index))
    print('num clusters = {}'.format(n_clusters))
    print('sync mode = {}'.format(sync_mode))

    storage = S3Storage()
    communicator = S3Communicator(storage, tmp_bucket, merged_bucket, n_workers, worker_index)

    # Reading data from S3
    read_start = time.time()
    lines = storage.load(file, data_bucket).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - read_start))

    parse_start = time.time()
    dataset = libsvm_dataset.from_lines(lines, n_features, dataset_type)
    train_set = dataset.ins_list
    np_dtype = train_set[0].to_dense().numpy().dtype
    centroid_shape = (n_clusters, n_features)

    print("parse data cost {} s".format(time.time() - parse_start))
    print("dataset type: {}, data type: {}, centroids shape: {}"
          .format(dataset_type, np_dtype, centroid_shape))

    # initialize centroids
    init_centroids_start = time.time()
    if worker_index == 0:
        centroids_np = sparse_centroid_to_numpy(train_set[0:n_clusters], n_clusters)
        storage.save(centroids_np.tobytes(), Prefix.KMeans_Init_Cent + "-1", merged_bucket)
    else:
        centroid_bytes = storage.load_or_wait(Prefix.KMeans_Init_Cent + "-1", merged_bucket).read()
        centroids_np = np.frombuffer(centroid_bytes, dtype=np_dtype).reshape(centroid_shape)

    centroids = torch.from_numpy(centroids_np)
    print("initial centroids cost {} s".format(time.time() - init_centroids_start))

    model = cluster_models.get_model(train_set, centroids, dataset_type, n_features, n_clusters)
    assert isinstance(model, SparseKMeans)

    train_start = time.time()
    for epoch in range(n_epochs):
        epoch_start = time.time()

        # rearrange data points
        model.find_nearest_cluster()

        local_cent = model.get_centroids("numpy").astype(np.float32).reshape(-1)
        local_cent_error = np.concatenate((local_cent.flatten(), np.array([model.error], dtype=np.float32)))
        epoch_cal_time = time.time() - epoch_start

        # sync local centroids and error
        epoch_sync_start = time.time()
        postfix = str(epoch)
        if sync_mode == "reduce":
            cent_error_merge = communicator.reduce_epoch(local_cent_error, postfix)
        elif sync_mode == "reduce_scatter":
            cent_error_merge = communicator.reduce_scatter_epoch(local_cent_error, postfix)

        print("one {} round cost {} s".format(sync_mode, time.time() - epoch_sync_start))

        cent_merge = cent_error_merge[:-1].reshape(centroid_shape) / float(n_workers)
        print("merged centroids shape = {}".format(cent_merge.shape))
        error_merge = cent_error_merge[-1] / float(n_workers)

        model.centroids = [torch.from_numpy(c).reshape(1, n_features).to_sparse()
                           for c in cent_merge]
        model.error = error_merge
        epoch_sync_time = time.time() - epoch_sync_start

        print("Epoch[{}] Worker[{}], error = {}, cost {} s, cal cost {} s, sync cost {} s"
              .format(epoch, worker_index, model.error,
                      time.time() - epoch_start, epoch_cal_time, epoch_sync_time))

        if model.error < threshold:
            break

    if worker_index == 0:
        storage.clear(tmp_bucket)
        storage.clear(merged_bucket)

    print("Worker[{}] finishes training: Error = {}, cost {} s"
          .format(worker_index, model.error, time.time() - train_start))
    return
