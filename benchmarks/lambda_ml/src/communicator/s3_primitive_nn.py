import urllib
import numpy as np
import pickle

from src.storage.s3.s3_type import S3Storage


def async_reduce(storage, input_bytes, bucket_name, object_name):
    assert isinstance(storage, S3Storage)

    storage.save(input_bytes, object_name, bucket_name)

    new_model = storage.load_or_wait(object_name, bucket_name, 0.1).read()
    new_model_np = pickle.loads(new_model)

    return new_model_np


def reduce_batch(storage, input_bytes, tmp_bucket, merged_bucket, n_workers, worker_index, postfix):
    assert isinstance(storage, S3Storage)

    postfix_splits = postfix.split("_")
    curr_epoch = int(postfix_splits[0])
    curr_batch = int(postfix_splits[1])

    # put object to s3, format of key: workerID_epoch_batch
    key = "{}_{}".format(worker_index, postfix)
    storage.save(input_bytes, key, tmp_bucket)

    merged_value = []

    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            objects = storage.list(tmp_bucket)
            if objects is not None:
                delete_list = []
                for obj in objects:
                    file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[-2]
                    key_batch = key_splits[-1]
                    if key_epoch == str(curr_epoch) and key_batch == str(curr_batch):
                        data_bytes = storage.load(file_key, tmp_bucket).read()
                        pickle_data = pickle.loads(data_bytes)
                        for i in range(len(pickle_data)):
                            if n_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        n_files = n_files + 1
                        delete_list.append(file_key)
                storage.delete(delete_list, tmp_bucket)
        # average gradients
        merged_value = [value / float(n_workers) for value in merged_value]
        # write the merged data back to s3
        merged_file_name = 'merged_' + postfix
        storage.save(pickle.dumps(merged_value), merged_file_name, merged_bucket)
        delete_expired_batch(storage, merged_bucket, curr_epoch, curr_batch)

    # read merged data
    merged_file_name = 'merged_' + postfix
    merged_data_bytes = storage.load_or_wait(merged_file_name, merged_bucket, 0.1).read()
    merged_data_np = pickle.loads(merged_data_bytes)

    return merged_data_np


def reduce_epoch(storage, input_bytes, tmp_bucket, merged_bucket, n_workers, worker_index, postfix):
    assert isinstance(storage, S3Storage)

    curr_epoch = int(postfix)

    # put object to s3, format of key: workerID_epoch
    key = "{}_{}".format(worker_index, postfix)
    storage.save(input_bytes, key, tmp_bucket)

    merged_value = []

    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            objects = storage.list(tmp_bucket)
            if objects is not None:
                delete_list = []
                for obj in objects:
                    file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[-1]
                    if key_epoch == str(curr_epoch):
                        data_bytes = storage.load(file_key, tmp_bucket).read()
                        data = pickle.loads(data_bytes)
                        for i in range(len(data)):
                            if n_files == 0:
                                merged_value.append(np.zeros(data[i].shape, dtype=data[i].dtype))
                            merged_value[i] = merged_value[i] + data[i]
                        n_files = n_files + 1
                        delete_list.append(file_key)
                storage.delete(delete_list, tmp_bucket)
        # average weights
        merged_value = [value / float(n_workers) for value in merged_value]
        # write the merged data back to s3
        merged_file_name = 'merged_' + postfix
        storage.save(pickle.dumps(merged_value), merged_file_name, merged_bucket)
        delete_expired_epoch(storage, merged_bucket, curr_epoch)

    # read merged data
    merged_file_name = 'merged_' + postfix
    merged_data_bytes = storage.load_or_wait(merged_file_name, merged_bucket, 0.1).read()
    merged_data_np = pickle.loads(merged_data_bytes)

    return merged_data_np


# delete the merged values of the *current or older* steps
def delete_expired_batch(storage, bucket_name, cur_epoch, cur_batch):
    objects = storage.list(bucket_name)
    if objects is not None:
        file_names = []
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            key_splits = file_key.split("_")
            key_batch = int(key_splits[-1])
            key_epoch = int(key_splits[-2])
            if key_epoch < cur_epoch or (key_epoch == cur_epoch and key_batch < cur_batch):
                file_names.append(file_key)
        if len(file_names) >= 1:
            storage.delete(file_names, bucket_name)
    return True


def delete_expired_epoch(storage, bucket_name, cur_epoch):
    objects = storage.list(bucket_name)
    if objects is not None:
        file_names = []
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            key_splits = file_key.split("_")
            key_epoch = int(key_splits[-1])
            if key_epoch < cur_epoch:
                file_names.append(file_key)
        if len(file_names) >= 1:
            storage.delete(file_names, bucket_name)
    return True


def reduce_scatter_batch(storage, vector, tmp_bucket, merged_bucket, num_workers, my_rank, postfix):
    assert isinstance(storage, S3Storage)

    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers

    postfix_splits = postfix.split("_")
    curr_epoch = postfix_splits[0]
    curr_batch = postfix_splits[1]

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

            # format of key in tmp-bucket: chunkID_workerID_epoch_batch
            storage.save(vector[offset: offset + length].tobytes(), key + '_' + postfix, tmp_bucket)

    # read and aggregate the corresponding chunk
    num_files = 0
    while num_files < num_workers - 1:
        objects = storage.list(tmp_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # if it's the chunk I care and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(my_rank) and key_splits[2] == curr_epoch and key_splits[3] == curr_batch:
                    data = storage.load(file_key, tmp_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    my_chunk = my_chunk + bytes_data
                    num_files += 1
                    storage.delete(file_key, tmp_bucket)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    storage.save(my_chunk.tobytes(), str(my_rank) + '_' + postfix, merged_bucket)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[my_rank] = my_chunk

    num_merged_files = 0
    already_read_files = []

    while num_merged_files < num_workers - 1:
        objects = storage.list(merged_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")
                # key format in merged_bucket: chunkID_epoch_batch
                # if not file_key.startswith(str(my_rank)) and file_key not in already_read:
                # key_splits = file_key.split("_")
                if key_splits[0] != str(my_rank) and key_splits[1] == curr_epoch and \
                        key_splits[2] == curr_batch and file_key not in already_read_files:
                    data = storage.load(file_key, merged_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    merged_value[int(key_splits[0])] = bytes_data
                    already_read_files.append(file_key)
                    num_merged_files += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result


def reduce_scatter_epoch(storage, vector, tmp_bucket, merged_bucket, num_workers, my_rank, postfix):
    assert isinstance(storage, S3Storage)

    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers

    cur_epoch = int(postfix)

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
            # format of key in tmp-bucket: chunkID_workerID_epoch
            storage.save(vector[offset: offset + length].tobytes(), key + '_' + postfix, tmp_bucket)

    # read and aggregate the corresponding chunk
    num_files = 0

    while num_files < num_workers - 1:
        objects = storage.list(tmp_bucket)

        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # if it's the chunk I care and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(my_rank) and key_splits[2] == str(cur_epoch):
                    data = storage.load(file_key, tmp_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    my_chunk = my_chunk + bytes_data
                    num_files += 1
                    storage.delete(file_key, tmp_bucket)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    storage.save(my_chunk.tobytes(), str(my_rank) + '_' + postfix, merged_bucket)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[my_rank] = my_chunk

    num_merged_files = 0
    already_read_files = []

    while num_merged_files < num_workers - 1:
        objects = storage.list(merged_bucket)

        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # key format in merged_bucket: chunkID_epoch
                # if not file_key.startswith(str(my_rank)) and file_key not in already_read:
                if key_splits[0] != str(my_rank) and key_splits[1] == str(cur_epoch) \
                        and file_key not in already_read_files:

                    data = storage.load(file_key, merged_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)

                    merged_value[int(key_splits[0])] = bytes_data

                    already_read_files.append(file_key)
                    num_merged_files += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result
