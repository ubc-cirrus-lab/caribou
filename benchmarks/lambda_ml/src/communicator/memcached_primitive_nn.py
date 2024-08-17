import urllib
import numpy as np
import pickle

from src.storage import MemcachedStorage


def async_reduce(storage, input_bytes, bucket_name, object_name):
    assert isinstance(storage, MemcachedStorage)

    storage.save_v2(input_bytes, object_name, bucket_name)

    new_model = storage.load_or_wait_v2(object_name, bucket_name, 0.1)
    new_model_np = pickle.loads(new_model)

    return new_model_np


def reduce_batch(storage, input_bytes, tmp_bucket, merged_bucket, num_workers, worker_index, cur_epoch, cur_batch):
    assert isinstance(storage, MemcachedStorage)

    postfix = "{}_{}".format(cur_epoch, cur_batch)

    # put object to memcached, format of key: tmp-bucket_workerID_epoch_batch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(input_bytes, key, tmp_bucket)

    merged_value = []

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        candidate_keys = ["{}_{}_{}".format(tmp_bucket, w, postfix) for w in range(num_workers)]
        while num_files < num_workers:
            objects = storage.list(candidate_keys)
            if objects is not None:
                delete_keys = []
                for file_key, value in objects.items():
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[-2]
                    key_batch = key_splits[-1]
                    if key_epoch == str(cur_epoch) and key_batch == str(cur_batch):
                        pickle_data = pickle.loads(value)
                        for i in range(len(pickle_data)):
                            if num_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        num_files += 1
                    delete_keys.append(file_key)
                    candidate_keys.remove(file_key)
                storage.delete(delete_keys)
        # average gradients
        merged_value = [value / float(num_workers) for value in merged_value]
        # write the merged data back to memcached
        merged_file_key = postfix
        storage.save_v2(pickle.dumps(merged_value), merged_file_key, merged_bucket)
        delete_expired_batch(storage, merged_bucket, cur_epoch, cur_batch)

    merged_file_key = postfix
    merged_data = storage.load_or_wait_v2(merged_file_key, merged_bucket, 0.1)
    merged_data_np = pickle.loads(merged_data)

    return merged_data_np


def reduce_epoch(storage, input_bytes, tmp_bucket, merged_bucket, num_workers, worker_index, cur_epoch):
    assert isinstance(storage, MemcachedStorage)

    postfix = str(cur_epoch)

    # put object to memcache, format of key: tmp-bucket_workerID_epoch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(input_bytes, key, tmp_bucket)

    merged_value = []

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        candidate_keys = ["{}_{}_{}".format(tmp_bucket, w, postfix) for w in range(num_workers)]
        while num_files < num_workers:
            objects = storage.list(candidate_keys)
            if objects is not None:
                delete_keys = []
                for file_key, data_bytes in objects.items():
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[-1]
                    if key_epoch == str(cur_epoch):
                        pickle_data = pickle.loads(data_bytes)
                        for i in range(len(pickle_data)):
                            if num_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        num_files += 1
                        delete_keys.append(file_key)
                        candidate_keys.remove(file_key)
                storage.delete(delete_keys)
        # average weights
        merged_value = [value / float(num_workers) for value in merged_value]
        # write the merged data back to memcached
        merged_file_key = postfix
        storage.save_v2(pickle.dumps(merged_value), merged_file_key, merged_bucket)
        delete_expired_epoch(storage, merged_bucket, cur_epoch)

    # read merged data
    merged_file_key = postfix
    merged_data_bytes = storage.load_or_wait_v2(merged_file_key, merged_bucket, 0.1)
    merged_data_np = pickle.loads(merged_data_bytes)

    return merged_data_np


# delete the merged values of the *current or older* steps
def delete_expired_batch(storage, bucket_name, cur_epoch, cur_batch):
    assert isinstance(storage, MemcachedStorage)
    candidate_keys = []
    for epoch in range(cur_epoch):
        for batch in range(cur_batch):
            candidate_keys.append("{}_{}_{}".format(bucket_name, epoch, batch))
    storage.delete(candidate_keys)
    return True


def delete_expired_epoch(storage, bucket_name, cur_epoch):
    assert isinstance(storage, MemcachedStorage)
    candidate_keys = []
    for epoch in range(cur_epoch):
        candidate_keys.append("{}_{}".format(bucket_name, epoch))
    storage.delete(candidate_keys)
    return True


def reduce_scatter_batch(storage, input_bytes, tmp_bucket, merged_bucket, num_workers, worker_index, cur_epoch, cur_batch):
    assert isinstance(storage, MemcachedStorage)

    postfix = "{}_{}".format(cur_epoch, cur_batch)

    # put object to memcached, format of key: bucket_workerID_epoch_batch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(input_bytes, key, tmp_bucket)

    merged_value = []

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        candidate_keys = ["{}_{}_{}".format(tmp_bucket, w, postfix) for w in range(num_workers)]
        while num_files < num_workers:
            objects = storage.list(candidate_keys)
            if objects is not None:
                delete_keys = []
                for file_key, value in objects.items():
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[-2]
                    key_batch = key_splits[-1]
                    if key_epoch == str(cur_epoch) and key_batch == str(cur_batch):
                        pickle_data = pickle.loads(value)
                        for i in range(len(pickle_data)):
                            if num_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        num_files += 1
                    delete_keys.append(file_key)
                    candidate_keys.remove(file_key)
                storage.delete(delete_keys)
        # average gradients
        merged_value = [value / float(num_workers) for value in merged_value]
        # write the merged data back to memcached
        merged_file_key = postfix
        storage.save_v2(pickle.dumps(merged_value), merged_file_key, merged_bucket)
        delete_expired_batch(storage, merged_bucket, cur_epoch, cur_batch)

    merged_file_key = postfix
    merged_data = storage.load_or_wait_v2(merged_file_key, merged_bucket, 0.1)
    merged_data_np = pickle.loads(merged_data)

    return merged_data_np


def reduce_scatter_epoch(storage, input_bytes, tmp_bucket, merged_bucket, num_workers, worker_index, cur_epoch):
    assert isinstance(storage, MemcachedStorage)

    postfix = str(cur_epoch)

    # put object to memcache, format of key: tmp-bucket_workerID_epoch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(input_bytes, key, tmp_bucket)

    merged_value = []

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        candidate_keys = ["{}_{}_{}".format(tmp_bucket, w, postfix) for w in range(num_workers)]
        while num_files < num_workers:
            objects = storage.list(candidate_keys)
            if objects is not None:
                delete_list = []
                for file_key, data_bytes in objects.items():
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[-1]
                    if key_epoch == str(cur_epoch):
                        pickle_data = pickle.loads(data_bytes)
                        for i in range(len(pickle_data)):
                            if num_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        num_files += 1
                        delete_list.append(file_key)
                        candidate_keys.remove(file_key)
                storage.delete(delete_list)
        # average weights
        merged_value = [value / float(num_workers) for value in merged_value]
        # write the merged data back to memcached
        merged_file_key = postfix
        storage.save_v2(pickle.dumps(merged_value), merged_file_key, merged_bucket)
        delete_expired_epoch(storage, merged_bucket, cur_epoch)

    # read merged data
    merged_file_key = postfix
    merged_data_bytes = storage.load_or_wait_v2(merged_file_key, merged_bucket, 0.1)
    merged_data_np = pickle.loads(merged_data_bytes)

    return merged_data_np
