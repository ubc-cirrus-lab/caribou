import urllib

import numpy as np
import pickle

from archived.s3 import list_bucket_objects
from archived.s3.get_object import get_object, get_object_or_wait
from archived.s3 import put_object
from archived.s3.delete_object import delete_object
from archived.s3 import delete_objects


def scatter_reduce(vector, tmp_bucket, merged_bucket, num_workers, myrank, postfix):
    
    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers
    curr_epoch = postfix.split("_")[0]
    curr_batch = postfix.split("_")[1]

    my_offset = (num_values_per_worker * myrank) + min(residue, myrank)
    my_length = num_values_per_worker + (1 if myrank < residue else 0)
    my_chunk = vector[my_offset : my_offset + my_length]

    # write partitioned vector to the shared memory, except the chunk charged by myself
    for i in range(num_workers):
        if i != myrank:
            offset = (num_values_per_worker * i) + min(residue, i)
            length = num_values_per_worker + (1 if i < residue else 0)
            # indicating the chunk number and which worker it comes from
            key = "{}_{}".format(i, myrank)
            # format of key in tmp-bucket: chunkID_workerID_epoch_batch
            put_object(tmp_bucket, key + '_' + postfix, vector[offset : offset + length].tobytes())
    
    # read and aggergate the corresponding chunk
    num_files = 0
    while num_files < num_workers - 1:
        objects = list_bucket_objects(tmp_bucket)
        if objects is not None:
            for obj in objects:

                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")
                
                # if it's the chunk I care and it is from the current step
                 # format of key in tmp-bucket: chunkID_workerID_epoch_batch
                if key_splits[0] == str(myrank) and key_splits[2] == curr_epoch and key_splits[3] == curr_batch:
                    
                    data = get_object(tmp_bucket, file_key).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)
                    my_chunk = my_chunk + bytes_data
                    num_files += 1
                    delete_object(tmp_bucket, file_key)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch_batch
    put_object(merged_bucket, str(myrank) + '_' + postfix, my_chunk.tobytes())

    # read other aggregated chunks
    merged_value = {}
    merged_value[myrank] = my_chunk
    
    num_merged_files = 0
    already_read = []
    while num_merged_files < num_workers - 1:
        objects = list_bucket_objects(merged_bucket)
        if objects is not None:
            for obj in objects:
                file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
                key_splits = file_key.split("_")
                #key format in merged_bucket: chunkID_epoch_batch
                if key_splits[0] != str(myrank) and key_splits[1] == curr_epoch and key_splits[2] == curr_batch and file_key not in already_read:
                # if not file_key.startswith(str(myrank)) and file_key not in already_read:
                    # key_splits = file_key.split("_")
                    data = get_object(merged_bucket, file_key).read()
                    bytes_data = np.frombuffer(data, dtype=vector.dtype)

                    merged_value[int(key_splits[0])] = bytes_data
                    
                    already_read.append(file_key)
                    num_merged_files += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))
        # elif k == myrank:
        #     result = np.concatenate((result, my_chunk))
        # else:
        #     result = np.concatenate((result, merged_value[k]))
    return result


# delete the merged values of the *current or older* steps
def delete_expired_merged(bucket_name, cur_epoch, cur_batch):
    objects = list_bucket_objects(bucket_name)
    if objects is not None:
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            key_splits = file_key.split("_")
            key_batch = int(key_splits[-1])
            key_epoch = int(key_splits[-2])
            if key_epoch < cur_epoch or (key_epoch == cur_epoch and key_batch < cur_batch):
                # print("delete object {} in bucket {}".format(file_key, bucket_name))
                delete_object(bucket_name, file_key)


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
    # if prefix == 'w_':
    merged_value = [value / float(num_workers) for value in merged_value]
    return merged_value


def put_merged(bucket_name, merged_value, prefix, file_postfix):
    # print('put merged weight {} to bucket {}'.format(w_prefix + file_postfix, bucket_name))
    put_object(bucket_name, prefix + file_postfix, pickle.dumps(merged_value))
    # print('put merged bias {} to bucket {}'.format(b_prefix + file_postfix, bucket_name))
    # put_object(bucket_name, b_prefix + file_postfix, b.tobytes())


def get_merged(bucket_name, prefix, file_postfix):
    # print('get merged weight {} in bucket {}'.format(w_prefix + file_postfix, bucket_name))
    merged_value = get_object_or_wait(bucket_name, prefix + file_postfix, 0.01).read()
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
                    # print("delete object {} in bucket {}".format(file_key, bucket_name))
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
