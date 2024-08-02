import numpy as np

from archived.elasticache.Memcached.list_keys import hlist_keys
from archived.elasticache.Memcached.set_object import hset_object
from archived.elasticache.Memcached.delete_keys import hdelete_keys


def avg_centroids(centroids_vec_list):
    cent_array = np.array(centroids_vec_list)
    return np.average(cent_array, axis=0)


def KeysPool(bucket,num_workers,postfix):
    candidate = []
    for worker_index in range(num_workers):
        candidate.append(f"{bucket}_{worker_index}_{postfix}")
    return candidate


def compute_average_centroids(endpoint, avg_cent_bucket, worker_cent_bucket, num_workers, shape, epoch, dt):
    num_files = 0
    centroids_vec_list = []
    error_list = []
    candidate = KeysPool(worker_cent_bucket,num_workers,epoch)
    while num_files < num_workers:
        #create a list of candidate for tmp-updates
        objects = hlist_keys(endpoint, candidate)
        if objects is not None:
            for key,value in objects.items():
                file_key = key
                candidate.remove(file_key)
                cent_with_error = np.frombuffer(value, dtype=dt)
                if num_files == 0:
                    avg = np.zeros(shape)
                    error = 0
                avg += cent_with_error[0:-1].reshape(shape)
                error += cent_with_error[-1]

                #centroids_vec_list.append(cent)
                #error_list.append(error)

                num_files = num_files + 1
                hdelete_keys(endpoint, file_key)

    print("All workers are ready.")
    avg = (avg/num_files).astype(dt)
    avg_error = (error/num_files).astype(dt)

    print(f"Write averaged centroids {avg} for {epoch}-th epoch to bucket {avg_cent_bucket}")
    print(f"Average error: {avg_error}")
    res = avg.reshape(-1)
    res = np.append(res, avg_error)
    hset_object(endpoint, avg_cent_bucket, f"avg-{epoch}", res.tobytes())
    return 1
