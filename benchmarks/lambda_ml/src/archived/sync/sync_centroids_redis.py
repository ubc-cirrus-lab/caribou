import numpy as np

from archived.elasticache import hlist_keys
from archived.elasticache import hget_object
from archived.elasticache import hset_object
from archived.elasticache.Redis.delete_keys import hdelete_keys


def avg_centroids(centroids_vec_list):
    cent_array = np.array(centroids_vec_list)
    return np.average(cent_array, axis=0)


def compute_average_centroids(endpoint, avg_cent_bucket, worker_cent_bucket, num_workers, shape, epoch, dt):
    num_files = 0
    centroids_vec_list = []
    error_list = []
    while num_files < num_workers:
        num_files = 0
        centroids_vec_list = []
        error_list = []
        objects = hlist_keys(endpoint, worker_cent_bucket)
        if objects is not None:
            for obj in objects:
                file_key = bytes.decode(obj)
                cent_with_error = np.frombuffer(hget_object(endpoint, worker_cent_bucket, file_key), dtype=dt)
                cent = cent_with_error[0:-1].reshape(shape)
                error = cent_with_error[-1]
                centroids_vec_list.append(cent)
                error_list.append(error)
                num_files = num_files + 1
        else:
            print(f"no object in the {worker_cent_bucket}")

    print("All workers are ready.")
    avg = avg_centroids(centroids_vec_list)
    avg_error = np.mean(np.array(error_list))
    clear_bucket(endpoint, worker_cent_bucket)

    print(f"Write averaged centroids {avg} for {epoch}-th epoch to bucket {avg_cent_bucket}")
    print(f"Average error: {avg_error}")
    res = avg.reshape(-1)
    res = np.append(res, avg_error)
    hset_object(endpoint, avg_cent_bucket, f"avg-{epoch}", res.tobytes())
    return 1


def clear_bucket(endpoint, bucket_name):
    objects = hlist_keys(endpoint, bucket_name)
    while objects != None:
        if len(objects) > 1:
            print("delete files {} in bucket {}".format(objects, bucket_name))
            hdelete_keys(endpoint, bucket_name, objects)
        objects = hlist_keys(endpoint, bucket_name)
    return True

