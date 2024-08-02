import urllib
import numpy as np

from src.storage.redis.redis_type import RedisStorage


def async_reduce(storage, vector, bucket_name, vector_name):
    assert isinstance(storage, RedisStorage)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype

    data = storage.load_or_wait_v2(vector_name, bucket_name, 0.1).read()
    new_vec = np.frombuffer(data, dtype=vec_dtype).reshape(vec_shape)
    storage.save_v2(vector.tobytes(), vector_name, bucket_name)

    return new_vec


def reduce_batch(storage, vector, tmp_bucket, merged_bucket, num_workers, worker_index, postfix):
    assert isinstance(storage, RedisStorage)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    postfix_splits = postfix.split("_")
    curr_epoch = int(postfix_splits[0])
    curr_batch = int(postfix_splits[1])

    # put object to s3, format of key: workerID_epoch_batch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(vector.tobytes(), key, tmp_bucket)

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        while num_files < num_workers:
            objects = storage.list_v2(tmp_bucket)
            if objects is not None:
                delete_list = []
                for obj in objects:
                    file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[1]
                    key_batch = key_splits[2]
                    if key_epoch == str(curr_epoch) and key_batch == str(curr_batch):
                        data = storage.load_v2(file_key, tmp_bucket).read()
                        bytes_data = np.frombuffer(data, dtype=vec_dtype)
                        tmp_vec = bytes_data.reshape(vec_shape)
                        merged_vec += tmp_vec
                        num_files += 1
                        delete_list.append(file_key)
                storage.delete_v2(delete_list, tmp_bucket)
        # write the merged data back to s3
        merged_file_name = 'merged_' + postfix
        storage.save_v2(merged_vec.tobytes(), merged_file_name, merged_bucket)
        delete_expired_batch(storage, merged_bucket, curr_epoch, curr_batch)
    else:
        merged_file_name = 'merged_' + postfix
        merged_data = storage.load_or_wait_v2(merged_file_name, merged_bucket, 0.1).read()
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


def reduce_epoch(storage, vector, tmp_bucket, merged_bucket, num_workers, worker_index, postfix):
    assert isinstance(storage, RedisStorage)

    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    curr_epoch = int(postfix)

    # put object to s3, format of key: workerID_epoch
    key = "{}_{}".format(worker_index, postfix)
    storage.save_v2(vector.tobytes(), key, tmp_bucket)

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        while num_files < num_workers:
            objects = storage.list_v2(tmp_bucket)
            if objects is not None:
                delete_list = []
                for obj in objects:
                    file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[1]
                    if key_epoch == str(curr_epoch):
                        data = storage.load_v2(file_key, tmp_bucket).read()
                        bytes_data = np.frombuffer(data, dtype=vec_dtype)
                        tmp_vec = bytes_data.reshape(vec_shape)
                        merged_vec += tmp_vec
                        num_files += 1
                        delete_list.append(file_key)
                storage.delete_v2(delete_list, tmp_bucket)
        # write the merged data back to s3
        storage.save_v2(merged_vec.tobytes(), 'merged_' + postfix, merged_bucket)
        delete_expired_epoch(storage, merged_bucket, curr_epoch)
    else:
        merged_file_name = 'merged_' + postfix
        merged_data = storage.load_or_wait_v2(merged_file_name, merged_bucket, 0.1).read()
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


# delete the merged values of the *current or older* steps
def delete_expired_batch(storage, bucket_name, cur_epoch, cur_batch):
    assert isinstance(storage, RedisStorage)
    objects = storage.list_v2(bucket_name)
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
            storage.delete_v2(file_names, bucket_name)
    return True


def delete_expired_epoch(storage, bucket_name, cur_epoch):
    assert isinstance(storage, RedisStorage)
    objects = storage.list_v2(bucket_name)
    if objects is not None:
        file_names = []
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            key_splits = file_key.split("_")
            key_epoch = int(key_splits[-1])
            if key_epoch < cur_epoch:
                file_names.append(file_key)
        if len(file_names) >= 1:
            storage.delete_v2(file_names, bucket_name)
    return True


def reduce_scatter_batch(storage, vector, tmp_bucket, merged_bucket, num_workers, my_rank, postfix):
    assert isinstance(storage, RedisStorage)

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
            storage.save_v2(vector[offset: offset + length].tobytes(), key + '_' + postfix, tmp_bucket)

    # read and aggregate the corresponding chunk
    num_files = 0
    while num_files < num_workers - 1:
        objects = storage.list_v2(tmp_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # if it's the chunk I care and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(my_rank) and key_splits[2] == curr_epoch and key_splits[3] == curr_batch:
                    data = storage.load_v2(file_key, tmp_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    my_chunk = my_chunk + bytes_data
                    num_files += 1
                    storage.delete_v2(file_key, tmp_bucket)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    storage.save_v2(my_chunk.tobytes(), str(my_rank) + '_' + postfix, merged_bucket)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[my_rank] = my_chunk

    num_merged_files = 0
    already_read_files = []

    while num_merged_files < num_workers - 1:
        objects = storage.list_v2(merged_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")
                # key format in merged_bucket: chunkID_epoch_batch
                # if not file_key.startswith(str(my_rank)) and file_key not in already_read:
                # key_splits = file_key.split("_")
                if key_splits[0] != str(my_rank) and key_splits[1] == curr_epoch and \
                        key_splits[2] == curr_batch and file_key not in already_read_files:
                    data = storage.load_v2(file_key, merged_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    merged_value[int(key_splits[0])] = bytes_data
                    already_read_files.append(file_key)
                    num_merged_files += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result


def reduce_scatter_batch_multi_bucket(storage, vector, tmp_bucket_prefix, merged_bucket_prefix,
                                      num_buckets, num_workers, my_rank, postfix):
    assert isinstance(storage, RedisStorage)

    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers

    postfix_splits = postfix.split("_")
    curr_epoch = postfix.split("_")[0]
    curr_batch = postfix.split("_")[1]

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

            tmp_bucket_ind = i % num_buckets
            tmp_bucket = "{}-{}".format(tmp_bucket_prefix, tmp_bucket_ind)

            # format of key in tmp-bucket: chunkID_workerID_epoch_batch
            storage.save_v2(vector[offset: offset + length].tobytes(), key + '_' + postfix, tmp_bucket)

    # read and aggregate the corresponding chunk
    num_files = 0
    tmp_bucket_ind = my_rank % num_buckets
    tmp_bucket = "{}-{}".format(tmp_bucket_prefix, tmp_bucket_ind)
    print("worker [{}] read and aggregate the corresponding chunks in bucket {}"
          .format(my_rank, tmp_bucket))

    while num_files < num_workers - 1:
        objects = storage.list_v2(tmp_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # if it's the chunk I care and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(my_rank) and key_splits[2] == curr_epoch and key_splits[3] == curr_batch:
                    data = storage.load_v2(file_key, tmp_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    my_chunk = my_chunk + bytes_data
                    num_files += 1
                    storage.delete_v2(file_key, tmp_bucket)

    merged_bucket_ind = my_rank % num_buckets
    my_merged_bucket = "{}-{}".format(merged_bucket_prefix, merged_bucket_ind)
    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    storage.save_v2(my_chunk.tobytes(), str(my_rank) + '_' + postfix, my_merged_bucket)

    # read other aggregated chunks
    merged_value = {my_rank: my_chunk}

    bucket_num_objs = []
    if num_workers % num_buckets == 0:
        bucket_num_objs = [num_workers / num_buckets for _ in range(num_buckets)]
    else:
        for i in range(num_workerss % num_buckets):
            bucket_num_objs.append(num_workers / num_buckets + 1)
        for i in range(num_workers % num_buckets, num_workers):
            bucket_num_objs.append(num_workers / num_buckets)   # check boundary
    # do not count responsible chunk
    bucket_num_objs[my_rank % num_buckets] -= 1
    print("bucket num objs = {}".format(bucket_num_objs))

    num_merged_files = 0
    already_read_files = []
    bucket_num_merged = [0 for _ in range(num_buckets)]

    while num_merged_files < num_workers - 1:
        for i in range(num_buckets):
            if bucket_num_merged[i] < bucket_num_objs[i]:
                merged_bucket = "{}-{}".format(merged_bucket_prefix, i)
                objects = storage.list_v2(merged_bucket)
                while objects is not None:
                    for obj in objects:
                        file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                        key_splits = file_key.split("_")
                        # key format in merged_bucket: chunkID_epoch_batch
                        # if not file_key.startswith(str(my_rank)) and file_key not in already_read:
                        if key_splits[0] != str(my_rank) and key_splits[1] == curr_epoch \
                                and key_splits[2] == curr_batch and file_key not in already_read_files:
                            data = storage.load_v2(file_key, merged_bucket).read()
                            bytes_data = np.frombuffer(data, dtype=vector.dtype)

                            merged_value[int(key_splits[0])] = bytes_data

                            already_read_files.append(file_key)
                            bucket_num_merged[i] += 1
                            num_merged_files += 1
                    objects = storage.list_v2(merged_bucket)

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result


def reduce_scatter_epoch(storage, vector, tmp_bucket, merged_bucket, num_workers, my_rank, postfix):
    assert isinstance(storage, RedisStorage)

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
            storage.save_v2(vector[offset: offset + length].tobytes(), key + '_' + postfix, tmp_bucket)

    # read and aggregate the corresponding chunk
    num_files = 0

    while num_files < num_workers - 1:
        objects = storage.list_v2(tmp_bucket)

        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # if it's the chunk I care and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(my_rank) and key_splits[2] == str(cur_epoch):
                    data = storage.load_v2(file_key, tmp_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    my_chunk = my_chunk + bytes_data
                    num_files += 1
                    storage.delete_v2(file_key, tmp_bucket)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    storage.save_v2(my_chunk.tobytes(), str(my_rank) + '_' + postfix, merged_bucket)

    # read other aggregated chunks
    merged_value = dict()
    merged_value[my_rank] = my_chunk

    num_merged_files = 0
    already_read_files = []

    while num_merged_files < num_workers - 1:
        objects = storage.list_v2(merged_bucket)

        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # key format in merged_bucket: chunkID_epoch
                # if not file_key.startswith(str(my_rank)) and file_key not in already_read:
                if key_splits[0] != str(my_rank) and key_splits[1] == str(cur_epoch) \
                        and file_key not in already_read_files:

                    data = storage.load_v2(file_key, merged_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)

                    merged_value[int(key_splits[0])] = bytes_data

                    already_read_files.append(file_key)
                    num_merged_files += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result


def reduce_scatter_epoch_multi_bucket(storage, vector, tmp_bucket_prefix, merged_bucket_prefix,
                                      num_buckets, num_workers, my_rank, postfix):
    assert isinstance(storage, RedisStorage)

    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers

    curr_epoch = postfix

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

            tmp_bucket_ind = i % num_buckets
            tmp_bucket = "{}-{}".format(tmp_bucket_prefix, tmp_bucket_ind)

            # format of key in tmp-bucket: chunkID_workerID_epoch_batch
            storage.save_v2(vector[offset: offset + length].tobytes(), key + '_' + postfix, tmp_bucket)

    # read and aggregate the corresponding chunk
    num_files = 0
    tmp_bucket_ind = my_rank % num_buckets
    tmp_bucket = "{}-{}".format(tmp_bucket_prefix, tmp_bucket_ind)
    print("worker [{}] read and aggregate the corresponding chunks in bucket {}"
          .format(my_rank, tmp_bucket))

    while num_files < num_workers - 1:
        objects = storage.list_v2(tmp_bucket)

        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")

                # if it's the chunk I care and it is from the current step
                # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(my_rank) and key_splits[2] == str(curr_epoch):
                    data = storage.load_v2(file_key, tmp_bucket).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    my_chunk = my_chunk + bytes_data
                    num_files += 1
                    storage.delete_v2(file_key, tmp_bucket)

    merged_bucket_ind = my_rank % num_buckets
    merged_bucket = "{}-{}".format(merged_bucket_prefix, merged_bucket_ind)
    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    storage.save_v2(my_chunk.tobytes(), str(my_rank) + '_' + postfix, merged_bucket)

    # read other aggregated chunks
    merged_value = {my_rank: my_chunk}

    bucket_num_objs = []
    if num_workers % num_buckets == 0:
        bucket_num_objs = [num_workers / num_buckets for _ in range(num_buckets)]
    else:
        for i in range(num_buckets % num_buckets):
            num_buckets.append(num_workers / num_buckets + 1)
        for i in range(num_buckets % num_buckets, num_buckets):
            num_buckets.append(num_workers / num_buckets)  # check boundary
    # do not count responsible chunk
    bucket_num_objs[my_rank % num_buckets] -= 1

    num_merged_files = 0
    already_read_files = []
    bucket_num_merged = [0 for _ in range(num_buckets)]

    while num_merged_files < num_workers - 1:
        for i in range(num_buckets):
            if bucket_num_merged[i] < bucket_num_objs[i]:
                merged_bucket = "{}-{}".format(merged_bucket_prefix, i)
                objects = storage.list_v2(merged_bucket)

                if objects is not None:
                    for obj in objects:
                        file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                        key_splits = file_key.split("_")

                        # key format in merged_bucket: chunkID_epoch
                        # if not file_key.startswith(str(myrank)) and file_key not in already_read:
                        if key_splits[0] != str(my_rank) and key_splits[1] == str(curr_epoch) \
                                and file_key not in already_read_files:
                            data = storage.load_v2(file_key, merged_bucket).read()
                            bytes_data = np.frombuffer(data, dtype=vector.dtype)

                            merged_value[int(key_splits[0])] = bytes_data
                            already_read_files.append(file_key)
                            bucket_num_merged[i] += 1
                            num_merged_files += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result
