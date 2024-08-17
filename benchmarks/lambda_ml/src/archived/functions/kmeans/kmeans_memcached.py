import urllib.parse
import logging
import time
import numpy as np

from archived.sync import compute_average_centroids
from archived.elasticache.Memcached import hget_object_or_wait
from archived.elasticache.Memcached import hset_object
from archived.s3.get_object import get_object
from archived.s3 import put_object
from archived.elasticache.Memcached import memcached_init
from data_loader.libsvm_dataset import DenseDatasetWithLines, SparseDatasetWithLines
from archived.functions import store_centroid_as_numpy, process_centroid, get_new_centroids

# setting
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    avg_error = np.iinfo(np.int16).max

    num_features = event['num_features']
    num_clusters = event['num_clusters']
    worker_cent_bucket = event["worker_cent_bucket"]
    avg_cent_bucket = event["avg_cent_bucket"]
    num_epochs = event["num_epochs"]
    threshold = event["threshold"]
    dataset_type = event["dataset_type"]
    elastic_location = event["elasticache"]
    elastic_endpoint = memcached_init(elastic_location)
    print(elastic_endpoint)
    #Reading data from S3
    bucket_name = event['bucket_name']
    key = urllib.parse.unquote_plus(event['key'], encoding='utf-8')
    logger.info(f"Reading training data from bucket = {bucket_name}, key = {key}")
    key_splits = key.split("_")
    num_worker = int(key_splits[-1])
    worker_index = int(key_splits[0])

    event_start = time.time()
    file = get_object(bucket_name, key).read().decode('utf-8').split("\n")
    s3_end = time.time()
    logger.info(f"Getting object from s3 takes {s3_end - event_start}s")

    if dataset_type == "dense":
        # dataset is stored as numpy array
        dataset = DenseDatasetWithLines(file, num_features).ins_np
        dt = dataset.dtype
        centroid_shape = (num_clusters, dataset.shape[1])
    else:
        # dataset is sparse, stored as sparse tensor
        dataset = SparseDatasetWithLines(file, num_features)
        first_entry = dataset.ins_list[0].to_dense().numpy()
        dt = first_entry.dtype
        centroid_shape = (num_clusters, first_entry.shape[1])
    parse_end = time.time()
    logger.info(f"Parsing dataset takes {parse_end - s3_end}s")
    logger.info(
        f"worker index: {worker_index},Dataset: {dataset_type}, dtype: {dt}. Centroids shape: {centroid_shape}. num_features: {num_features}")

    if worker_index == 0:
        if dataset_type == "dense":
            centroids = dataset[0:num_clusters].reshape(-1)
            hset_object(elastic_endpoint, avg_cent_bucket, "initial", centroids.tobytes())
            centroids = centroids.reshape(centroid_shape)
        else:
            centroids = store_centroid_as_numpy(dataset.ins_list[0:num_clusters], num_clusters)
            hset_object(elastic_endpoint, avg_cent_bucket, "initial", centroids.tobytes())
    else:
        cent = hget_object_or_wait(elastic_endpoint, avg_cent_bucket, "initial", 0.00001)
        centroids = process_centroid(cent, num_clusters, dt)
        #centroids = np.frombuffer(cent,dtype=dt)
        if centroid_shape != centroids.shape:
            logger.error("The shape of centroids does not match.")
        logger.info(f"Waiting for initial centroids takes {time.time() - parse_end} s")


    training_start = time.time()
    sync_time = 0
    for epoch in range(num_epochs):
        logger.info(f"{worker_index}-th worker in {epoch}-th epoch")
        epoch_start = time.time()
        if epoch != 0:
            last_epoch = epoch - 1
            cent_with_error = hget_object_or_wait(elastic_endpoint, avg_cent_bucket, f"avg-{last_epoch}", 0.00001)
            wait_end = time.time()
            if worker_index != 0:
                logger.info(f"Wait for centroid for {epoch}-th epoch. Takes {wait_end - epoch_start}")
                sync_time +=  wait_end - epoch_start
            avg_error, centroids = process_centroid(cent_with_error, num_clusters, dt, True)
        if avg_error >= threshold:
            print("get new centro")
            res = get_new_centroids(dataset, dataset_type, centroids, epoch, num_features, num_clusters)
            #dt = res.dtype
            sync_start = time.time()
            success = hset_object(elastic_endpoint, worker_cent_bucket, f"{worker_index}_{epoch}", res.tobytes())

            if worker_index == 0 and success:

                compute_average_centroids(elastic_endpoint,avg_cent_bucket, worker_cent_bucket, num_worker, centroid_shape, epoch, dt)
                logger.info(f"Waiting for all workers takes {time.time() - sync_start} s")
                if epoch!=0:
                    sync_time += time.time() - sync_start

        else:
            print("sync time = {}".format(sync_time))
            logger.info(f"{worker_index}-th worker finished training. Error = {avg_error}, centroids = {centroids}")
            logger.info(f"Whole process time : {time.time() - training_start}")
            return
        print("sync time = {}".format(sync_time))
        put_object("kmeans-time","time_{}".format(worker_index),np.asarray(sync_time).tostring())
