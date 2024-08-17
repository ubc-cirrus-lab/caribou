import urllib
import numpy as np

from src.storage import MemcachedStorage


def async_reduce(storage, vector, bucket_name, vector_name):
    assert isinstance(storage, MemcachedStorage)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype

    data = storage.load_or_wait_v2(vector_name, bucket_name, 0.1)
    new_vec = np.frombuffer(data, dtype=vec_dtype).reshape(vec_shape)
    storage.save_v2(vector.tobytes(), vector_name, bucket_name)

    return new_vec


def reduce_batch(storage, vector, tmp_bucket, merged_bucket, num_workers, worker_index, cur_epoch, cur_batch):
    assert isinstance(storage, MemcachedStorage)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    postfix = "{}_{}".format(cur_epoch, cur_batch)

    # put object to memcached, format of key: bucket_workerID_epoch_batch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(vector.tobytes(), key, tmp_bucket)

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
                        bytes_data = np.frombuffer(value, dtype=vec_dtype)
                        tmp_vec = bytes_data.reshape(vec_shape)
                        merged_vec += tmp_vec
                        num_files += 1
                    delete_keys.append(file_key)
                    candidate_keys.remove(file_key)
                storage.delete(delete_keys)
        # write the merged data back to memcache
        merged_file_key = postfix
        storage.save_v2(merged_vec.tobytes(), merged_file_key, merged_bucket)
        delete_expired_batch(storage, merged_bucket, cur_epoch, cur_batch)
    else:
        merged_file_key = postfix
        merged_data = storage.load_or_wait_v2(merged_file_key, merged_bucket, 0.1)
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


def reduce_epoch(storage, vector, tmp_bucket, merged_bucket, num_workers, worker_index, cur_epoch):
    assert isinstance(storage, MemcachedStorage)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    postfix = str(cur_epoch)

    # put object to memcache, format of key: bucket_workerID_epoch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(vector.tobytes(), key, tmp_bucket)

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        candidate_keys = ["{}_{}_{}".format(tmp_bucket, w, postfix) for w in range(num_workers)]
        print("candidate keys = {}".format(candidate_keys))
        while num_files < num_workers:
            objects = storage.list(candidate_keys)
            if objects is not None:
                delete_keys = []
                for file_key, value in objects.items():
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[-1]
                    if key_epoch == str(cur_epoch):
                        print("merge key {}".format(file_key))
                        tmp_vec = np.frombuffer(value, dtype=vec_dtype).reshape(vec_shape)
                        merged_vec += tmp_vec
                        num_files += 1
                        delete_keys.append(file_key)
                        candidate_keys.remove(file_key)
                storage.delete(delete_keys)
        # write the merged data back to memcache
        merged_file_key = postfix
        storage.save_v2(merged_vec.tobytes(), merged_file_key, merged_bucket)
        delete_expired_epoch(storage, merged_bucket, cur_epoch)
    else:
        merged_file_key = postfix
        merged_data = storage.load_or_wait_v2(merged_file_key, merged_bucket, 0.1)
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


# delete the merged values of the *current or older* steps
def delete_expired_batch(storage, bucket_name, cur_epoch, cur_batch):
    assert isinstance(storage, MemcachedStorage)
    candidate_keys = []
    for epoch in range(cur_epoch):
        for batch in range(cur_batch):
            candidate_keys.append("{}_{}_{}".format(bucket_name, epoch, batch))
    storage.delete(candidate_keys)
    #print("delete keys {}".format(candidate_keys))
    return True


def delete_expired_epoch(storage, bucket_name, cur_epoch):
    assert isinstance(storage, MemcachedStorage)
    candidate_keys = []
    for epoch in range(cur_epoch):
        candidate_keys.append("{}_{}".format(bucket_name, epoch))
    storage.delete(candidate_keys)
    #print("delete keys {}".format(candidate_keys))
    return True


def reduce_scatter_batch(storage, vector, tmp_bucket, merged_bucket, num_workers, my_rank, cur_epoch, cur_batch):
    assert isinstance(storage, MemcachedStorage)

    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers

    postfix = "{}_{}".format(cur_epoch, cur_batch)

    my_offset = (num_values_per_worker * my_rank) + min(residue, my_rank)
    my_length = num_values_per_worker + (1 if my_rank < residue else 0)
    my_chunk = vector[my_offset: my_offset + my_length]

    # write partitioned vector to the shared storage, except the chunk charged by myself
    for i in range(num_workers):
        if i != my_rank:
            offset = (num_values_per_worker * i) + min(residue, i)
            length = num_values_per_worker + (1 if i < residue else 0)
            # indicating the chunk number and which worker it comes from
            key = "{}_{}".format(i, my_rank)
            # format of key: tmp-bucket_chunkID_workerID_epoch_batch
            storage.save_v2(vector[offset: offset + length].tobytes(), "{}_{}".format(key, postfix), tmp_bucket)

    candidate_keys = []
    for worker_index in range(num_workers):
        if worker_index != my_rank:
            candidate_keys.append("{}_{}_{}_{}".format(tmp_bucket, my_rank, worker_index, postfix))

    # read and aggregate the corresponding chunk
    num_files = 0
    while num_files < num_workers - 1:
        objects = storage.list(candidate_keys)
        if objects is not None:
            delete_keys = []
            for file_key, value in objects.items():
                key_splits = file_key.split("_")
                # if it's the chunk I care and it is from the current step
                # format of key: tmp-bucket_chunkID_workerID_epoch_batch
                if key_splits[1] == str(my_rank) \
                        and key_splits[-2] == str(cur_epoch) and key_splits[-1] == str(cur_batch):
                    value_np = np.frombuffer(value, dtype=vector.dtype)
                    my_chunk = my_chunk + value_np
                    num_files += 1
                    delete_keys.append(file_key)
                    candidate_keys.remove(file_key)
            storage.delete(delete_keys)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    storage.save_v2(my_chunk.tobytes(), str(my_rank) + '_' + postfix, merged_bucket)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[my_rank] = my_chunk

    candidate_keys = []
    for worker_index in range(num_workers):
        if worker_index != my_rank:
            candidate_keys.append("{}_{}_{}".format(merged_bucket, worker_index, postfix))

    num_merged_keys = 0
    read_keys = []

    while num_merged_keys < num_workers - 1:
        objects = storage.list(candidate_keys)
        if objects is not None:
            for file_key, value in objects.items():
                key_splits = file_key.split("_")
                # key format: merged-bucket_chunkID_epoch_batch
                if key_splits[1] != str(my_rank) and key_splits[-2] == str(cur_epoch) and \
                        key_splits[-1] == str(cur_batch) and file_key not in read_keys:
                    value_np = np.frombuffer(value, dtype=vector.dtype)
                    merged_value[int(key_splits[1])] = value_np
                    read_keys.append(file_key)
                    num_merged_keys += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result


def reduce_scatter_epoch(storage, vector, tmp_bucket, merged_bucket, num_workers, my_rank, cur_epoch):
    assert isinstance(storage, MemcachedStorage)

    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers

    postfix = str(cur_epoch)

    my_offset = (num_values_per_worker * my_rank) + min(residue, my_rank)
    my_length = num_values_per_worker + (1 if my_rank < residue else 0)
    my_chunk = vector[my_offset: my_offset + my_length]

    # write partitioned vector to the shared memory, except the chunk charged by myself
    for i in range(num_workers):
        if i != my_rank:
            offset = (num_values_per_worker * i) + min(residue, i)
            length = num_values_per_worker + (1 if i < residue else 0)
            # indicating the chunk number and which worker it comes from
            key = "{}_{}".format(i, my_rank)
            # format of key in tmp-bucket: tmp-bucket_chunkID_workerID_epoch
            storage.save_v2(vector[offset: offset + length].tobytes(), "{}_{}".format(key, postfix), tmp_bucket)

    # read and aggregate the corresponding chunk
    num_files = 0
    candidate_keys = []
    for worker_index in range(num_workers):
        if worker_index != my_rank:
            candidate_keys.append("{}_{}_{}_{}".format(tmp_bucket, my_rank, worker_index, postfix))

    while num_files < num_workers - 1:
        objects = storage.list(candidate_keys)
        if objects is not None:
            delete_list = []
            for file_key, value in objects.items():
                key_splits = file_key.split("_")
                # if it's the chunk I care and it is from the current step
                # format of key in tmp-bucket: tmp-bucket_chunkID_workerID_epoch
                if key_splits[1] == str(my_rank) and key_splits[-1] == str(cur_epoch):
                    vec_data = np.frombuffer(value, dtype=vector.dtype)
                    my_chunk = my_chunk + vec_data
                    num_files += 1
                    delete_list.append(file_key)
                    candidate_keys.remove(file_key)
            storage.delete(delete_list)

    # write the aggregated chunk back
    # key format in merged_bucket: merged-bucket_chunkID_epoch
    storage.save_v2(my_chunk.tobytes(), "{}_{}".format(my_rank, postfix), merged_bucket)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[my_rank] = my_chunk

    num_merged_keys = 0
    already_read_keys = []

    candidate_keys = []
    for worker_index in range(num_workers):
        if worker_index != my_rank:
            candidate_keys.append("{}_{}_{}".format(merged_bucket, worker_index, postfix))

    while num_merged_keys < num_workers - 1:
        objects = storage.list(candidate_keys)
        if objects is not None:
            for file_key, value in objects.items():
                key_splits = file_key.split("_")
                # key format in merged_bucket: merged-bucket_chunkID_epoch
                # if not file_key.startswith(str(my_rank)) and file_key not in already_read:
                if key_splits[1] != str(my_rank) and key_splits[-1] == str(cur_epoch) \
                        and file_key not in already_read_keys:
                    vec_data = np.frombuffer(value, dtype=vector.dtype)
                    merged_value[int(key_splits[1])] = vec_data
                    already_read_keys.append(file_key)
                    candidate_keys.remove(file_key)
                    num_merged_keys += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result
