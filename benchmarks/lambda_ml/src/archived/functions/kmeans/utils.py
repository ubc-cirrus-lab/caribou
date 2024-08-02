import numpy as np
import logging
import time

from archived.old_model import Kmeans
from archived.pytorch_model import SparseKmeans

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def avg_centroids(centroids_vec_list):
    cent_array = np.array(centroids_vec_list)
    return np.average(cent_array, axis=0)


def store_centroid_as_numpy(centroid_sparse_tensor, nr_cluster):
    cent_lst = [centroid_sparse_tensor[i].to_dense().numpy() for i in range(nr_cluster)]
    centroid = np.array(cent_lst)
    return centroid


def process_centroid(centroid_byte, nr_cluster, dt, with_error=False):
    centroid_np = np.frombuffer(centroid_byte, dtype=dt)
    if with_error:
        centroid_size = centroid_np.shape[0] - 1
        return centroid_np[-1], centroid_np[0:-1].reshape(nr_cluster, int(centroid_size/nr_cluster))
    else:
        centroid_size = centroid_np.shape[0]
        return centroid_np.reshape(nr_cluster, int(centroid_size/nr_cluster))


def get_new_centroids(dataset, dataset_type, old_centroids, epoch, num_features, num_clusters, dt):
    compute_start = time.time()
    if dataset_type == "dense":
        model = Kmeans(dataset, old_centroids)
    else:
        model = SparseKmeans(dataset.ins_list, old_centroids, num_features, num_clusters)
    model.find_nearest_cluster()
    res = model.get_centroids("numpy").reshape(-1)

    compute_end = time.time()
    logger.info(f"Epoch = {epoch} Error = {model.error}. Computation time: {compute_end - compute_start}")
    res = np.append(res, model.error).astype(dt)
    return res
