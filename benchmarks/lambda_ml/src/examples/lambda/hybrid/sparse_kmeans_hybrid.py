import urllib.parse
import numpy as np
import time

import torch

from data_loader import libsvm_dataset

from utils.constants import Prefix, Synchronization
from src.storage.s3.s3_type import S3Storage

from model import cluster_models
from model.cluster_models import KMeans, SparseKMeans

from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


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
    assert dataset_type == "sparse_libsvm"
    n_features = event['n_features']

    # ps setting
    host = event['host']
    port = event['port']

    # hyper-parameter
    n_clusters = event['n_clusters']
    n_epochs = event["n_epochs"]
    threshold = event["threshold"]
    sync_mode = event["sync_mode"]
    n_workers = event["n_workers"]
    worker_index = event['worker_index']
    assert sync_mode.lower() == Synchronization.Reduce

    print('data bucket = {}'.format(data_bucket))
    print("file = {}".format(file))
    print('number of workers = {}'.format(n_workers))
    print('worker index = {}'.format(worker_index))
    print('num clusters = {}'.format(n_clusters))
    print('host = {}'.format(host))
    print('port = {}'.format(port))

    # Set thrift connection
    # Make socket
    transport = TSocket.TSocket(host, port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    t_client = ParameterServer.Client(protocol)
    # Connect!
    transport.open()
    # test thrift connection
    ps_client.ping(t_client)
    print("create and ping thrift server >>> HOST = {}, PORT = {}".format(host, port))

    # Reading data from S3
    read_start = time.time()
    storage = S3Storage()
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

    # register model
    model_name = Prefix.KMeans_Cent
    model_length = centroid_shape[0] * centroid_shape[1] + 1
    ps_client.register_model(t_client, worker_index, model_name, model_length, n_workers)
    ps_client.exist_model(t_client, model_name)
    print("register and check model >>> name = {}, length = {}".format(model_name, model_length))

    init_centroids_start = time.time()
    ps_client.can_pull(t_client, model_name, 0, worker_index)
    ps_model = ps_client.pull_model(t_client, model_name, 0, worker_index)
    if worker_index == 0:
        centroids_np = sparse_centroid_to_numpy(train_set[0:n_clusters], n_clusters)
        ps_client.can_push(t_client, model_name, 0, worker_index)
        ps_client.push_grad(t_client, model_name,
                            np.append(centroids_np.flatten(), 1000.).astype(np.double) - np.asarray(ps_model).astype(np.double),
                            1., 0, worker_index)
    else:
        centroids_np = np.zeros(centroid_shape)
        ps_client.can_push(t_client, model_name, 0, worker_index)
        ps_client.push_grad(t_client, model_name,
                            np.append(centroids_np.flatten(), 0).astype(np.double),
                            0, 0, worker_index)
    ps_client.can_pull(t_client, model_name, 1, worker_index)
    ps_model = ps_client.pull_model(t_client, model_name, 1, worker_index)
    cur_centroids = np.array(ps_model[0:-1]).astype(np.float32).reshape(centroid_shape)
    cur_error = float(ps_model[-1])
    print("initial centroids cost {} s".format(time.time() - init_centroids_start))

    model = cluster_models.get_model(train_set, torch.from_numpy(cur_centroids), dataset_type,
                                     n_features, n_clusters)

    train_start = time.time()
    for epoch in range(1, n_epochs + 1):
        epoch_start = time.time()

        # local computation
        model.find_nearest_cluster()
        local_cent = model.get_centroids("numpy").reshape(-1)
        local_cent_error = np.concatenate((local_cent.astype(np.double).flatten(),
                                           np.array([model.error], dtype=np.double)))
        epoch_cal_time = time.time() - epoch_start

        # push updates
        epoch_comm_start = time.time()
        last_cent_error = np.concatenate((cur_centroids.astype(np.double).flatten(),
                                          np.array([cur_error], dtype=np.double)))
        ps_model_inc = local_cent_error - last_cent_error
        ps_client.can_push(t_client, model_name, epoch, worker_index)
        ps_client.push_grad(t_client, model_name,
                            ps_model_inc, 1. / n_workers, epoch, worker_index)

        # pull new model
        ps_client.can_pull(t_client, model_name, epoch + 1, worker_index)   # sync all workers
        ps_model = ps_client.pull_model(t_client, model_name, epoch + 1, worker_index)
        model.centroids = [torch.from_numpy(c).reshape(1, n_features).to_sparse()
                           for c in np.array(ps_model[0:-1]).astype(np.float32).reshape(centroid_shape)]

        model.error = float(ps_model[-1])
        cur_centroids = model.get_centroids("numpy")
        cur_error = model.error

        epoch_comm_time = time.time() - epoch_comm_start

        print("Epoch[{}] Worker[{}], error = {}, cost {} s, cal cost {} s, sync cost {} s"
              .format(epoch, worker_index, model.error,
                      time.time() - epoch_start, epoch_cal_time, epoch_comm_time))

        if model.error < threshold:
            break

    print("Worker[{}] finishes training: Error = {}, cost {} s"
          .format(worker_index, model.error, time.time() - train_start))
    return
