import numpy as np
import torch
import time

from data_loader.libsvm_dataset import SparseDatasetWithLines


def _l2_dist_sq(x1, x2):
    diff = torch.sparse.FloatTensor.sub(x1, x2)
    sq_diff = torch.sparse.FloatTensor.mul(diff, diff)
    sum = torch.sparse.sum(sq_diff)
    return sum


def _closest_centroid(cent, data):
    start = time.time()
    argmin_dist = np.zeros(len(data))
    for i in range(len(data)):
        min_sum = np.inf
        idx = 0
        for j in range(len(cent)):
            tmp = _l2_dist_sq(data[i], cent[j])
            if tmp < min_sum:
                idx = j
                min_sum = tmp
        argmin_dist[i] = idx
    print(f"Find closest centroids takes {time.time() - start}s")
    return np.array(argmin_dist, np.uint8)


def _move_centroids(num_clusters, data, closest, cent):
    start = time.time()
    c_mean = [torch.sparse.FloatTensor(cent[0].size()[0], cent[0].size()[1]) for i in range(num_clusters)]
    c_count = [0 for i in range(num_clusters)]
    for i in range(len(data)):
        c_mean[closest[i]] = torch.sparse.FloatTensor.add(data[i], c_mean[closest[i]])
        c_count[closest[i]] += 1
    for i in range(num_clusters):
        c_mean[i] = torch.sparse.FloatTensor.div(c_mean[i], c_count[i])
    print(f"Allocate data to new centroids takes {time.time() - start}s")
    return c_mean


def _get_error(old_cent, new_cent, num_clusters):
    start = time.time()
    tmp = _l2_dist_sq(new_cent[0], old_cent[0])
    for i in range(1, num_clusters):
        tmp = torch.sparse.FloatTensor.add(_l2_dist_sq(new_cent[i], old_cent[i]), tmp)
    print(f"Compute error takes {time.time() - start}s")
    return torch.sparse.FloatTensor.div(tmp, num_clusters)


class SparseKmeans(object):
    def __init__(self, _data, _centroids, _nr_feature, _nr_cluster, _error=np.iinfo(np.int16).max):
        self.data = _data
        self.nr_feature = _nr_feature
        self.centroids = [c.clone().detach().reshape(1, _nr_feature).to_sparse() for c in _centroids]
        self.nr_cluster = _nr_cluster
        self.error = _error
        self.model = torch.zeros(self.nr_feature, 1)

    def find_nearest_cluster(self):
        print("Start computing kmeans...")
        closest = _closest_centroid(self.centroids, self.data)
        new_centroids = _move_centroids(self.nr_cluster, self.data, closest, self.centroids)
        self.error = _get_error(self.centroids, new_centroids, self.nr_cluster)
        self.centroids = new_centroids
        return

    def get_centroids(self, centroids_type):
        if centroids_type == "sparse_tensor":
            return self.centroids
        if centroids_type == "numpy":
            cent_lst = [self.centroids[i].to_dense().numpy() for i in range(self.nr_cluster)]
            centroid = np.array(cent_lst).reshape(self.nr_cluster, self.nr_feature)
            return centroid
        if centroids_type == "dense_tensor":
            cent_tensor_lst = [self.centroids[i].to_dense() for i in range(self.nr_cluster)]
            return torch.stack(cent_tensor_lst)


if __name__ == "__main__":
    train_file = "../dataset/agaricus_127d_train.libsvm"
    test_file = "../dataset/agaricus_127d_test.libsvm"
    dim = 127
    train_data = SparseDatasetWithLines(train_file, dim)
    test_data = SparseDatasetWithLines(test_file, dim)
    nr_cluster = 10
    centroids = train_data.ins_list[:nr_cluster]
    kmeans_model = SparseKmeans(train_data, centroids, dim, nr_cluster)
    kmeans_model.find_nearest_cluster()
