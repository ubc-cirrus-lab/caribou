import urllib.parse
import numpy as np
import time

from data_loader import libsvm_dataset

from utils.constants import Prefix, Synchronization
from src.storage import S3Storage, DynamoTable
from src.storage.dynamo import dynamo_operator
from communicator import DynamoCommunicator

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


def new_centroids_with_error(dataset, dataset_type, old_centroids, epoch, n_features, n_clusters, data_type):
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
    assert dataset_type == "dense_libsvm"
    n_features = event['n_features']
    n_workers = event["n_workers"]
    worker_index = event['worker_index']
    tmp_table_name = event['tmp_table_name']
    merged_table_name = event['merged_table_name']
    key_col = event['key_col']

    # hyper-parameter
    n_clusters = event['n_clusters']
    n_epochs = event["n_epochs"]
    threshold = event["threshold"]
    sync_mode = event["sync_mode"]
    assert sync_mode.lower() in [Synchronization.Reduce, Synchronization.Reduce_Scatter]

    print('data bucket = {}'.format(data_bucket))
    print("file = {}".format(file))
    print('number of workers = {}'.format(n_workers))
    print('worker index = {}'.format(worker_index))
    print('num clusters = {}'.format(n_clusters))
    print('sync mode = {}'.format(sync_mode))

    s3_storage = S3Storage()
    dynamo_client = dynamo_operator.get_client()
    tmp_table = DynamoTable(dynamo_client, tmp_table_name)
    merged_table = DynamoTable(dynamo_client, merged_table_name)
    communicator = DynamoCommunicator(dynamo_client, tmp_table, merged_table, key_col, n_workers, worker_index)

    # Reading data from S3
    read_start = time.time()
    lines = s3_storage.load(file, data_bucket).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - read_start))

    parse_start = time.time()
    dataset = libsvm_dataset.from_lines(lines, n_features, dataset_type).ins_np
    data_type = dataset.dtype
    centroid_shape = (n_clusters, dataset.shape[1])
    print("parse data cost {} s".format(time.time() - parse_start))
    print("dataset type: {}, dtype: {}, Centroids shape: {}, num_features: {}"
          .format(dataset_type, data_type, centroid_shape, n_features))

    init_centroids_start = time.time()
    if worker_index == 0:
        centroids = dataset[0:n_clusters]
        merged_table.save(centroids.tobytes(), Prefix.KMeans_Init_Cent + "-1", key_col)
    else:
        centroid_bytes = (merged_table.load_or_wait(Prefix.KMeans_Init_Cent + "-1", key_col, 0.1))['value'].value
        centroids = centroid_bytes2np(centroid_bytes, n_clusters, data_type)
        if centroid_shape != centroids.shape:
            raise Exception("The shape of centroids does not match.")

    print("initialize centroids takes {} s".format(time.time() - init_centroids_start))

    model = cluster_models.get_model(dataset, centroids, dataset_type, n_features, n_clusters)

    train_start = time.time()
    for epoch in range(n_epochs):
        epoch_start = time.time()

        # rearrange data points
        model.find_nearest_cluster()

        local_cent = model.get_centroids("numpy").reshape(-1)
        local_cent_error = np.concatenate((local_cent.flatten(), np.array([model.error], dtype=np.float32)))
        epoch_cal_time = time.time() - epoch_start

        # sync local centroids and error
        epoch_comm_start = time.time()

        if sync_mode == "reduce":
            cent_error_merge = communicator.reduce_epoch(local_cent_error, epoch)
        elif sync_mode == "reduce_scatter":
            cent_error_merge = communicator.reduce_scatter_epoch(local_cent_error, epoch)

        cent_merge = cent_error_merge[:-1].reshape(centroid_shape) / float(n_workers)
        error_merge = cent_error_merge[-1] / float(n_workers)

        model.centroids = cent_merge
        model.error = error_merge
        epoch_comm_time = time.time() - epoch_comm_start

        print("one {} round cost {} s".format(sync_mode, epoch_comm_time))

        print("Epoch[{}] Worker[{}], error = {}, cost {} s, cal cost {} s, sync cost {} s"
              .format(epoch, worker_index, model.error,
                      time.time() - epoch_start, epoch_cal_time, epoch_comm_time))

        if model.error < threshold:
            break

    if worker_index == 0:
        tmp_table.clear(key_col)
        merged_table.clear(key_col)

    print("Worker[{}] finishes training: Error = {}, cost {} s"
          .format(worker_index, model.error, time.time() - train_start))
    return
