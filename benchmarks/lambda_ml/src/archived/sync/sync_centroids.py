import urllib
import numpy as np

from archived.s3.list_objects import list_bucket_objects
from archived.s3.get_object import get_object
from archived.s3 import put_object
from archived.s3 import delete_objects


def avg_centroids(centroids_vec_list):
    cent_array = np.array(centroids_vec_list)
    return np.average(cent_array, axis=0)


def compute_average_centroids(avg_cent_bucket, worker_cent_bucket, num_workers, shape, epoch, dt):
    num_files = 0
    centroids_vec_list = []
    error_list = []
    while num_files < num_workers:
        num_files = 0
        centroids_vec_list = []
        error_list = []
        objects = list_bucket_objects(worker_cent_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                data = get_object(worker_cent_bucket, file_key).read()
                cent_with_error = np.frombuffer(data, dtype=dt)
                cent = cent_with_error[0:-1].reshape(shape)
                error = cent_with_error[-1]
                centroids_vec_list.append(cent)
                error_list.append(error)
                num_files = num_files + 1
        else:
            print('No objects in {}'.format(worker_cent_bucket))

    avg = avg_centroids(centroids_vec_list)
    avg_error = np.mean(np.array(error_list))
    clear_bucket(worker_cent_bucket)

    print(f"Average error for {epoch}-th epoch: {avg_error}")
    res = avg.reshape(-1)
    res = np.append(res, avg_error).astype(dt)
    put_object(avg_cent_bucket, f"avg-{epoch}", res.tobytes())
    return 1


def clear_bucket(bucket_name):
    objects = list_bucket_objects(bucket_name)
    if objects is not None:
        file_names = []
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            file_names.append(file_key)
        if len(file_names) > 1:
            print("delete files {} in bucket {}".format(file_names, bucket_name))
            delete_objects(bucket_name, file_names)
    return True
