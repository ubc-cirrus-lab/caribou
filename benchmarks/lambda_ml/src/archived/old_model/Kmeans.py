import numpy as np
import logging
import torch

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _l2_dist(a, b, ax=1):
    return np.mean(np.linalg.norm(a - b, axis=ax))


def _closest_centroid(points, centroids):
    """returns an array containing the index to the nearest centroid for each point"""
    batch_size = 5000
    remaining = points.shape[0]
    current_batch = 0
    argmin_dist = []

    while True:
        if remaining <= batch_size:
            dist = np.full((remaining, centroids.shape[0]), np.inf)
            for i in range(centroids.shape[0]):
                dist[:, i] = np.sum(np.square(points[current_batch:current_batch + remaining] - centroids[i, :]),
                                    axis=1)
            argmin_dist.append(np.argmin(dist, axis=1))
            break
        else:
            dist = np.full((batch_size, centroids.shape[0]), np.inf)
            for i in range(centroids.shape[0]):
                dist[:, i] = np.sum(np.square(points[current_batch:current_batch + batch_size] - centroids[i, :]),
                                    axis=1)
            argmin_dist.append(np.argmin(dist, axis=1))
            current_batch += batch_size
            remaining -= batch_size

    res = argmin_dist[0]
    for i in range(1, len(argmin_dist)):
        res = np.concatenate((res, argmin_dist[i]), axis=None)
    return res


def _move_centroids(points, closest, centroids):
    """returns the new centroids assigned from the points closest to them"""
    x = np.array([points[closest == k].mean(axis=0) for k in range(centroids.shape[0])])
    return np.nan_to_num(x)


class Kmeans:
    def __init__(self, data, centroids, error=np.iinfo(np.int16).max, centroid_type='numpy'):
        self.X = data
        if centroid_type == 'numpy':
            self.centroids = centroids
        else:
            # input centroids is a dense tensor
            self.centroids = centroids.numpy()
        self.error = error


    def find_nearest_cluster(self):
        # centroids.shape: nr_clusters, data_dim
        closest = _closest_centroid(self.X, self.centroids)
        new_centroids_vec = _move_centroids(self.X, closest, self.centroids)
        self.error = _l2_dist(self.centroids, new_centroids_vec)
        self.centroids = new_centroids_vec
        return

    def get_centroids(self, centroids_type):
        if centroids_type == "numpy":
            return self.centroids
        if centroids_type == "dense_tensor":
            return torch.tensor(self.centroids)
