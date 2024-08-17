import time

import torch
import numpy as np


def get_model(dataset, centroids, dataset_type, n_features, n_cluster):
    if dataset_type == "dense_libsvm":
        return KMeans(dataset, centroids)
    elif dataset_type == "sparse_libsvm":
        return SparseKMeans(dataset, centroids, n_features, n_cluster)


class KMeans(object):

    def __init__(self, data, centroids, centroid_type='numpy'):
        self.X = data
        if centroid_type == 'numpy':
            self.centroids = centroids
        elif centroid_type == 'tensor':
            # input centroids is a pytorch tensor
            self.centroids = centroids.numpy()
        self.error = np.finfo(np.float32).max

    @staticmethod
    def euclidean_dist(a, b, axis=1):
        return np.mean(np.linalg.norm(a - b, axis=axis))

    def closest_centroid(self):
        """returns an array containing the index to the nearest centroid for each point"""
        batch_size = 5000
        remaining = self.X.shape[0]
        current_batch = 0
        argmin_dist = []

        while True:
            if remaining <= batch_size:
                dist = np.full((remaining, self.centroids.shape[0]), np.inf)
                for i in range(self.centroids.shape[0]):
                    dist[:, i] = np.sum(
                        np.square(self.X[current_batch:current_batch + remaining] - self.centroids[i, :]),
                        axis=1)
                argmin_dist.append(np.argmin(dist, axis=1))
                break
            else:
                dist = np.full((batch_size, self.centroids.shape[0]), np.inf)
                for i in range(self.centroids.shape[0]):
                    dist[:, i] = np.sum(
                        np.square(self.X[current_batch:current_batch + batch_size] - self.centroids[i, :]),
                        axis=1)
                argmin_dist.append(np.argmin(dist, axis=1))
                current_batch += batch_size
                remaining -= batch_size

        res = argmin_dist[0]
        for i in range(1, len(argmin_dist)):
            res = np.concatenate((res, argmin_dist[i]), axis=None)
        return res

    def update_centroids(self, closest):
        """returns the new centroids assigned from the points closest to them"""
        x = np.array([self.X[closest == k].mean(axis=0) for k in range(self.centroids.shape[0])])
        return np.nan_to_num(x)

    def find_nearest_cluster(self):
        # centroids.shape: nr_clusters, data_dim
        closest = self.closest_centroid()
        new_centroids = self.update_centroids(closest)
        self.error = self.euclidean_dist(self.centroids, new_centroids)
        self.centroids = new_centroids
        return

    def get_centroids(self, centroids_type):
        if centroids_type == "numpy":
            return self.centroids
        if centroids_type == "tensor":
            return torch.tensor(self.centroids)


class SparseKMeans(object):

    def __init__(self, _data, _centroids, _n_feature, _n_cluster):
        self.data = _data
        self.n_feature = _n_feature
        self.centroids = [c.clone().detach().reshape(1, self.n_feature).to_sparse() for c in _centroids]
        self.n_cluster = _n_cluster
        self.error = np.finfo(np.float32).max
        self.model = torch.zeros(self.n_feature, 1)

    @staticmethod
    def euclidean_dist(x1, x2):
        diff = torch.sparse.FloatTensor.sub(x1, x2)
        sq_diff = torch.sparse.FloatTensor.mul(diff, diff)
        dist_sum = torch.sparse.sum(sq_diff)
        # diff = torch.sub(x1.to_dense(), x2.to_dense())
        # sq_diff = torch.mul(diff, diff)
        # dist_sum = torch.sum(sq_diff)
        return dist_sum

    def closest_centroid(self):
        start = time.time()
        argmin_dist = np.zeros(len(self.data))
        for i in range(len(self.data)):
            min_sum = np.inf
            idx = 0
            for j in range(len(self.centroids)):
                tmp = self.euclidean_dist(self.data[i], self.centroids[j])
                if tmp < min_sum:
                    idx = j
                    min_sum = tmp
            argmin_dist[i] = idx
        print(f"Find closest centroids takes {time.time() - start}s")
        return np.array(argmin_dist, np.uint8)

    def move_centroids(self, closest):
        start = time.time()
        c_mean = [torch.sparse.FloatTensor(self.centroids[0].size()[0], self.centroids[0].size()[1])
                  for i in range(self.n_cluster)]
        c_count = [0 for i in range(self.n_cluster)]
        for i in range(len(self.data)):
            c_mean[closest[i]] = torch.sparse.FloatTensor.add(self.data[i], c_mean[closest[i]])
            c_count[closest[i]] += 1
        for i in range(self.n_cluster):
            c_mean[i] = torch.sparse.FloatTensor.div(c_mean[i], c_count[i])
        print(f"Allocate data to new centroids takes {time.time() - start}s")
        return c_mean

    def get_error(self, new_cent):
        start = time.time()
        tmp = self.euclidean_dist(new_cent[0], self.centroids[0])
        for i in range(1, self.n_cluster):
            tmp = torch.sparse.FloatTensor.add(self.euclidean_dist(new_cent[i], self.centroids[i]), tmp)
        print(f"Compute error takes {time.time() - start}s")
        return torch.sparse.FloatTensor.div(tmp, self.n_cluster)

    def find_nearest_cluster(self):
        print("Start computing kmeans...")
        closest = self.closest_centroid()
        new_centroids = self.move_centroids(closest)
        self.error = self.get_error(new_centroids).item()
        self.centroids = new_centroids
        return

    def get_centroids(self, centroids_type):
        if centroids_type == "sparse_tensor":
            return self.centroids
        elif centroids_type == "numpy":
            cent_lst = [self.centroids[i].to_dense().numpy() for i in range(self.n_cluster)]
            centroid_np = np.array(cent_lst).reshape(self.n_cluster, self.n_feature)
            return centroid_np
        elif centroids_type == "dense_tensor":
            cent_tensor_lst = [self.centroids[i].to_dense() for i in range(self.n_cluster)]
            return torch.stack(cent_tensor_lst)
        else:
            raise Exception("centroid type can only be sparse_tensor, dense_tensor, or numpy")
