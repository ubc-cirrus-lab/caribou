import urllib

import numpy as np
import pickle

from archived.s3 import list_bucket_objects
from archived.s3.get_object import get_object, get_object_or_wait
from archived.s3 import put_object
from archived.s3.delete_object import delete_object
from archived.s3 import delete_objects


def reduce_batch(vector, tmp_bucket, merged_bucket, num_workers, worker_index, postfix):
    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    postfix_splits = postfix.split("_")
    curr_epoch = int(postfix_splits[0])
    curr_batch = int(postfix_splits[1])

    # put object to s3, format of key: workerID_epoch_batch
    key = "{}_{}".format(worker_index, postfix)
    put_object(tmp_bucket, key, vector.tobytes())

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        while num_files < num_workers:
            objects = list_bucket_objects(tmp_bucket)
            if objects is not None:
                delete_list = []
                for obj in objects:
                    file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[1]
                    key_batch = key_splits[2]
                    if key_epoch == str(curr_epoch) and key_batch == str(curr_batch):
                        data = get_object(tmp_bucket, file_key).read()
                        bytes_data = np.frombuffer(data, dtype=vec_dtype)
                        tmp_vec = bytes_data.reshape(vec_shape)
                        merged_vec += tmp_vec
                        num_files += 1
                        delete_list.append(file_key)
                delete_objects(tmp_bucket, delete_list)
        # write the merged data back to s3
        merged_file_name = 'merged_' + postfix
        put_object(merged_bucket, merged_file_name, merged_vec.tobytes())
        delete_expired_merged_batch(merged_bucket, curr_epoch, curr_batch)
    else:
        merged_file_name = 'merged_' + postfix
        merged_data = get_object_or_wait(merged_bucket, merged_file_name, 0.1).read()
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


def reduce_epoch(vector, tmp_bucket, merged_bucket, num_workers, worker_index, postfix):
    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    curr_epoch = int(postfix)

    # put object to s3, format of key: workerID_epoch
    key = "{}_{}".format(worker_index, postfix)
    #print("put file {} to s3".format(key))
    put_object(tmp_bucket, key, vector.tobytes())

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        num_files = 0
        while num_files < num_workers:
            objects = list_bucket_objects(tmp_bucket)
            if objects is not None:
                delete_list = []
                for obj in objects:
                    file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                    key_splits = file_key.split("_")
                    key_epoch = key_splits[1]
                    if key_epoch == str(curr_epoch):
                        data = get_object(tmp_bucket, file_key).read()
                        bytes_data = np.frombuffer(data, dtype=vec_dtype)
                        tmp_vec = bytes_data.reshape(vec_shape)
                        merged_vec += tmp_vec
                        num_files += 1
                        delete_list.append(file_key)
                delete_objects(tmp_bucket, delete_list)
        # write the merged data back to s3
        put_object(merged_bucket, 'merged_' + postfix, merged_vec.tobytes())
        delete_expired_merged_epoch(merged_bucket, curr_epoch)
    else:
        merged_file_name = 'merged_' + postfix
        merged_data = get_object_or_wait(merged_bucket,merged_file_name, 0.1).read()
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


# delete the merged values of the *current or older* steps
def delete_expired_merged_batch(bucket_name, cur_epoch, cur_batch):
    objects = list_bucket_objects(bucket_name)
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
            #print("delete files {} in bucket {}".format(file_names, bucket_name))
            delete_objects(bucket_name, file_names)


def delete_expired_merged_epoch(bucket_name, cur_epoch):
    objects = list_bucket_objects(bucket_name)
    if objects is not None:
        file_names = []
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            key_splits = file_key.split("_")
            key_epoch = int(key_splits[-1])
            if key_epoch < cur_epoch:
                file_names.append(file_key)
        if len(file_names) >= 1:
            #print("delete files {} in bucket {}".format(file_names, bucket_name))
            delete_objects(bucket_name, file_names)


def merge_all_workers(bucket_name, num_workers, prefix):
    num_files = 0
    # merged_value = np.zeros(dshape, dtype=dtype)
    merged_value = []

    while num_files < num_workers:
        objects = list_bucket_objects(bucket_name)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                data_bytes = get_object(bucket_name, file_key).read()
                data = pickle.loads(data_bytes)

                for i in range(len(data)):
                    if num_files == 0:
                        merged_value.append(np.zeros(data[i].shape, dtype=data[i].dtype))
                    merged_value[i] = merged_value[i] + data[i]

                num_files = num_files + 1
                delete_object(bucket_name, file_key)

    # average weights
    if prefix == 'w_':
        merged_value = [value / float(num_workers) for value in merged_value]

    return merged_value


def put_merged(bucket_name, merged_value, prefix, file_postfix):
    # print('put merged weight {} to bucket {}'.format(w_prefix + file_postfix, bucket_name))
    put_object(bucket_name, prefix + file_postfix, pickle.dumps(merged_value))


def get_merged(bucket_name, prefix, file_postfix):
    # print('get merged weight {} in bucket {}'.format(w_prefix + file_postfix, bucket_name))
    merged_value = get_object_or_wait(bucket_name, prefix + file_postfix, 0.1).read()
    merged_value_np = pickle.loads(merged_value)
    # merged_value_np = np.frombuffer(merged_value, dtype=dtype).reshape(dshape)

    return merged_value_np


def delete_expired(bucket_name, cur_epoch, cur_batch, prefix):
    objects = list_bucket_objects(bucket_name)
    if objects is not None:
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            if file_key.startswith(prefix):
                key_splits = file_key.split("_")
                key_batch = int(key_splits[-1])
                key_epoch = int(key_splits[-2])
                if key_epoch < cur_epoch or (key_epoch == cur_epoch and key_batch < cur_batch):
                    print("delete object {} in bucket {}".format(file_key, bucket_name))
                    delete_object(bucket_name, file_key)


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
