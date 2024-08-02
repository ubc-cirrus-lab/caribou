import urllib
import numpy as np

from src.storage import DynamoTable


def async_reduce(table, vector, key_col, vector_name):
    assert isinstance(table, DynamoTable)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype

    data = table.load_or_wait(vector_name, key_col, 0.1)['value'].value
    new_vec = np.frombuffer(data, dtype=vec_dtype).reshape(vec_shape)
    table.save(vector.tobytes(), vector_name, key_col)

    return new_vec


def reduce_batch(tmp_table, merged_table, vector, key_col, n_workers, worker_index, cur_epoch, cur_batch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    # put object to tmp table, format of key: workerID_epoch_batch
    my_key = "{}_{}".format(cur_epoch, cur_batch)
    tmp_table.save(vector.tobytes(), "{}_{}".format(worker_index, my_key), key_col)

    # the first worker read and aggregate
    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            items = tmp_table.list()
            if items is not None and len(items) > 0:
                delete_keys = []
                for item in items:
                    tmp_key = item[key_col]
                    key_splits = tmp_key.split("_")
                    key_epoch = key_splits[-2]
                    key_batch = key_splits[-1]
                    if key_epoch == str(cur_epoch) and key_batch == str(cur_batch):
                        bytes_data = item['value'].value
                        tmp_vec = np.frombuffer(bytes_data, dtype=vec_dtype).reshape(vec_shape)
                        merged_vec += tmp_vec
                        n_files += 1
                        delete_keys.append(tmp_key)
                tmp_table.delete(delete_keys, key_col)
        # write the merged data to merged table
        merged_key = 'merged_{}'.format(my_key)
        merged_table.save(merged_vec.tobytes(), merged_key, key_col)
        delete_expired_batch(merged_table, key_col, cur_epoch, cur_batch)
    else:
        merged_key = 'merged_{}'.format(my_key)
        merged_data = merged_table.load_or_wait(merged_key, key_col, 0.1)['value'].value
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


def reduce_epoch(tmp_table, merged_table, vector, key_col, n_workers, worker_index, cur_epoch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    # put object to tmp table, format of key: workerID_epoch
    key = str(cur_epoch)
    tmp_table.save(vector.tobytes(), "{}_{}".format(worker_index, key), key_col)

    # the first worker read and aggregate
    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            items = tmp_table.list()
            if items is not None and len(items) > 0:
                delete_keys = []
                for item in items:
                    tmp_key = item[key_col]
                    key_splits = tmp_key.split("_")
                    key_epoch = key_splits[-1]
                    if key_epoch == str(cur_epoch):
                        bytes_data = item['value'].value
                        tmp_vec = np.frombuffer(bytes_data, dtype=vec_dtype).reshape(vec_shape)
                        merged_vec += tmp_vec
                        n_files += 1
                        delete_keys.append(tmp_key)
                tmp_table.delete(delete_keys, key_col)
        # write the merged data to merged table
        merged_key = 'merged_{}'.format(key)
        merged_table.save(merged_vec.tobytes(), merged_key, key_col)
        delete_expired_epoch(merged_table, key_col, cur_epoch)
    else:
        merged_key = 'merged_{}'.format(key)
        merged_data = merged_table.load_or_wait(merged_key, key_col, 0.1)['value'].value
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


# delete the merged values of the *current or older* steps
def delete_expired_batch(table, key_col, cur_epoch, cur_batch):
    assert isinstance(table, DynamoTable)
    items = table.list()
    if items is not None and len(items) > 0:
        delete_keys = []
        for item in items:
            key = item[key_col]
            key_splits = key.split("_")
            key_batch = int(key_splits[-1])
            key_epoch = int(key_splits[-2])
            if key_epoch < cur_epoch or (key_epoch == cur_epoch and key_batch < cur_batch):
                delete_keys.append(key)
        if len(delete_keys) >= 1:
            table.delete(delete_keys, key_col)
    return True


def delete_expired_epoch(table, key_col, cur_epoch):
    assert isinstance(table, DynamoTable)
    items = table.list()
    if items is not None and len(items) > 0:
        delete_keys = []
        for item in items:
            key = item[key_col]
            key_splits = key.split("_")
            key_epoch = int(key_splits[-1])
            if key_epoch < cur_epoch:
                delete_keys.append(key)
        if len(delete_keys) >= 1:
            table.delete(delete_keys, key_col)
    return True


def reduce_scatter_batch(tmp_table, merged_table, vector, key_col, n_workers, worker_index, cur_epoch, cur_batch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # vector is supposed to be a 1-d numpy array
    vec_size = vector.size
    vec_size_per_worker = vec_size // n_workers
    vec_size_residue = vec_size % n_workers

    postfix = "{}_{}".format(cur_epoch, cur_batch)

    my_offset = (vec_size_per_worker * worker_index) + min(vec_size_residue, worker_index)
    my_length = vec_size_per_worker + (1 if worker_index < vec_size_residue else 0)
    my_chunk = vector[my_offset: my_offset + my_length]
    my_chunk_shape = my_chunk.shape

    # write partitioned vector to the shared storage, except the chunk charged by myself
    for i in range(n_workers):
        if i != worker_index:
            offset = (vec_size_per_worker * i) + min(vec_size_residue, i)
            length = vec_size_per_worker + (1 if i < vec_size_residue else 0)
            # indicating the chunk number and which worker it comes from

            # format of key in tmp-bucket: chunkID_workerID_epoch_batch
            chunk_id = i
            tmp_key = "{}_{}_{}".format(chunk_id, worker_index, postfix)
            tmp_table.save(vector[offset: offset + length].tobytes(), tmp_key, key_col)

    # read and aggregate the corresponding chunk
    n_files = 0
    while n_files < n_workers - 1:
        tmp_items = tmp_table.list()
        if tmp_items is not None and len(tmp_items) > 0:
            delete_keys = []
            for tmp_item in tmp_items:
                tmp_key = tmp_item[key_col]
                key_splits = tmp_key.split("_")
                # if it's the responsible chunk and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(worker_index) \
                        and key_splits[-2] == str(cur_epoch) \
                        and key_splits[-1] == str(cur_batch):
                    bytes_data = tmp_item['value'].value
                    tmp_vec = np.frombuffer(bytes_data, dtype=vector.dtype).reshape(my_chunk_shape)
                    my_chunk = my_chunk + tmp_vec
                    n_files += 1
                    delete_keys.append(tmp_key)
            tmp_table.delete(delete_keys, key_col)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    merged_key = "{}_{}".format(worker_index, postfix)
    merged_table.save(my_chunk.tobytes(), merged_key, key_col)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[worker_index] = my_chunk

    n_merged_keys = 0
    read_keys = []

    while n_merged_keys < n_workers - 1:
        merged_items = merged_table.list()
        if merged_items is not None and len(merged_items) > 0:
            for merged_item in merged_items:
                merged_key = merged_item[key_col]
                key_splits = merged_key.split("_")
                # key format in merged_bucket: chunkID_epoch_batch
                # if not file_key.startswith(str(my_rank)) and merged_key not in already_read:
                if key_splits[0] != str(worker_index) and key_splits[-2] == str(cur_epoch) and \
                        key_splits[-1] == str(cur_batch) and merged_key not in read_keys:
                    bytes_data = merged_item['value'].value
                    merged_value[int(key_splits[0])] = np.frombuffer(bytes_data, dtype=vector.dtype)
                    read_keys.append(merged_key)
                    n_merged_keys += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, n_workers):
        result = np.concatenate((result, merged_value[k]))

    return result


def reduce_scatter_epoch(tmp_table, merged_table, vector, key_col, n_workers, worker_index, cur_epoch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # vector is supposed to be a 1-d numpy array
    vec_size = vector.size
    vec_size_per_worker = vec_size // n_workers
    vec_size_residue = vec_size % n_workers

    my_offset = (vec_size_per_worker * worker_index) + min(vec_size_residue, worker_index)
    my_length = vec_size_per_worker + (1 if worker_index < vec_size_residue else 0)
    my_chunk = vector[my_offset: my_offset + my_length]
    my_chunk_shape = my_chunk.shape

    # write partitioned vector to the shared memory, except the chunk charged by myself
    for i in range(n_workers):
        if i != worker_index:
            offset = (vec_size_per_worker * i) + min(vec_size_residue, i)
            length = vec_size_per_worker + (1 if i < vec_size_residue else 0)
            # indicating the chunk number and which worker it comes from
            chunk_id = i
            tmp_key = "{}_{}_{}".format(chunk_id, worker_index, cur_epoch)
            # format of key in tmp-bucket: chunkID_workerID_epoch
            tmp_table.save(vector[offset: offset + length].tobytes(), tmp_key, key_col)

    # read and aggregate the corresponding chunk
    n_merged_keys = 0

    while n_merged_keys < n_workers - 1:
        tmp_items = tmp_table.list()
        delete_keys = []
        if tmp_items is not None and len(tmp_items) > 0:
            for tmp_item in tmp_items:
                tmp_key = tmp_item[key_col]
                key_splits = tmp_key.split("_")
                # if it's the responsible chunk and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch
                if key_splits[0] == str(worker_index) and key_splits[-1] == str(cur_epoch):
                    bytes_data = tmp_item['value'].value
                    my_chunk = my_chunk + np.frombuffer(bytes_data, dtype=vector.dtype)
                    n_merged_keys += 1
            tmp_table.delete(delete_keys, key_col)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch
    merged_key = "{}_{}".format(worker_index, cur_epoch)
    merged_table.save(my_chunk.tobytes(), merged_key, key_col)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[worker_index] = my_chunk

    n_merged_keys = 0
    read_keys = []
    while n_merged_keys < n_workers - 1:
        merged_items = merged_table.list()
        if merged_items is not None and len(merged_items) > 0:
            for merged_item in merged_items:
                merged_key = merged_item[key_col]
                key_splits = merged_key.split("_")
                # key format in merged_bucket: chunkID_epoch
                # if not file_key.startswith(str(my_rank)) and merged_key not in already_read:
                if (key_splits[0]).isdigit() and key_splits[0] != str(worker_index) and key_splits[-1] == str(cur_epoch) \
                        and merged_key not in read_keys:
                    bytes_data = merged_item['value'].value
                    merged_value[int(key_splits[0])] = np.frombuffer(bytes_data, dtype=vector.dtype)
                    read_keys.append(merged_key)
                    n_merged_keys += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, n_workers):
        result = np.concatenate((result, merged_value[k]))

    return result
