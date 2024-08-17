import numpy as np
import pickle

from archived.elasticache.Memcached import hlist_keys
from archived.elasticache.Memcached import hget_object_or_wait
from archived.elasticache.Memcached import hset_object
from archived.elasticache.Memcached import hdelete_keys


def merge_w_b_layers(endpoint, bucket_name, num_workers, prefix):
    #whatever merges w/b grads or model
    num_files = 0
    merged_value = []
    candidate = []
    split = []
    for worker_index in range(num_workers):
        #split.append(bucket_name+"_"+prefix+str(worker_index))
        #if worker_index%20==0 and worker_index != 0:
        #    candidate.append(split)
        #    split = []
    #candidate.append(split)
        candidate.append(bucket_name+"_"+prefix+str(worker_index))
    #group = len(candidate)
    count = 0
    while num_files < num_workers :#or candidate != []:

        objects= hlist_keys(endpoint, candidate)#[count%group])

        while objects is not None :#or candidate != []:
            #file_keys = []
            print(objects.keys())
            for key,value in objects.items():
                file_key = key
                #index = file_key.split('_')[2]
                #del candidate[index]
                candidate.remove(file_key)
                data_bytes = value
                data = pickle.loads(data_bytes)
                for i in range(len(data)):
                    if num_files == 0:
                        merged_value.append(np.zeros(data[i].shape, dtype=data[i].dtype))
                    merged_value[i] = merged_value[i] + data[i]
                #print("{} is being deleted? {}".format(file_key,hdelete_keys(endpoint, [file_key])))
                num_files = num_files + 1
                hdelete_keys(endpoint, [file_key])
            #file_keys = list(objects.keys())
            objects= hlist_keys(endpoint, candidate)#[count%group])
            count = count +1
            print(num_workers-num_files)
    #clear_all(endpoint)
    # average weights
    if prefix == 'w_':
        merged_value = [value / float(num_workers) for value in merged_value]

    return merged_value


def put_merged_w_b_layers(endpoint, bucket_name, merged_value, prefix, file_postfix):
    # print('put merged weight {} to bucket {}'.format(w_prefix + file_postfix, bucket_name))
    print("merged operation = {}".format(hset_object(endpoint, bucket_name, prefix + file_postfix, pickle.dumps(merged_value))))
    # print('put merged bias {} to bucket {}'.format(b_prefix + file_postfix, bucket_name))
    # put_object(bucket_name, b_prefix + file_postfix, b.tobytes())


def get_merged_w_b_layers(endpoint, bucket_name, prefix, file_postfix):
    # print('get merged weight {} in bucket {}'.format(w_prefix + file_postfix, bucket_name))
    merged_value = hget_object_or_wait(endpoint, bucket_name, prefix + file_postfix, 0.000001)
    merged_value_np = pickle.loads(merged_value)
    # merged_value_np = np.frombuffer(merged_value, dtype=dtype).reshape(dshape)

    return merged_value_np


def delete_expired_w_b_layers(endpoint, bucket_name, cur_epoch, cur_batch, prefix, end):
    if cur_batch ==0:
        if cur_epoch != 0:
            expired = [bucket_name+"_"+prefix+str(cur_epoch-1)+"_"+str(end)]
        else:
            return
    else:
        expired = bucket_name+"_"+prefix+str(cur_epoch)+"_"+str(cur_batch-1)
    #expired = [bucket_name+"_"+prefix+str(cur_epoch)+"_"+str(cur_batch)]
    hdelete_keys(endpoint, [expired])


#mergence for single file
def merge_w_b_grads(endpoint, bucket_name, num_workers,
                    dtype, w_shape, b_shape,
                    w_grad_prefix="w_grad_", b_grad_prefix="b_grad_"):
    num_w_files = 0
    num_b_files = 0
    w_grad_sum = np.zeros(w_shape, dtype=dtype)
    b_grad_sum = np.zeros(b_shape, dtype=dtype)
    w_candidate = []
    b_candidate = []
    for worker_index in range(num_workers):
        w_candidate.append(bucket_name + "_" + w_grad_prefix + str(worker_index))
        b_candidate.append(bucket_name + "_" + b_grad_prefix + str(worker_index))
    candidate = w_candidate+b_candidate
    while num_w_files < num_workers or num_b_files < num_workers:
        file_keys = []
        objects = hlist_keys(endpoint,candidate)
        if objects is not None:
            print(objects.keys())
            for key,value in objects.items():
                #file_key = bytes.decode(obj)
                file_key = key
                file_keys.append(file_key)
                print("the name of the file being processed = {}".format(file_key))
                bytes_data = np.fromstring(value,dtype = dtype)
                if file_key.startswith(bucket_name+"_"+w_grad_prefix):
                    w_grad = bytes_data.reshape(w_shape)
                    #print("merge the {}-th weight grad {} in bucket {} = {}".format(num_w_files, file_key, bucket_name, w_grad[0][:5]))
                    w_grad_sum = w_grad_sum + w_grad
                    num_w_files = num_w_files + 1
                elif file_key.startswith(bucket_name+"_"+b_grad_prefix):
                    b_grad = bytes_data.reshape(b_shape)
                    #print("merge the {}-th bias grad {} in bucket {} = {}".format(num_b_files, file_key, bucket_name, b_grad))
                    b_grad_sum = b_grad_sum + b_grad
                    num_b_files = num_b_files + 1
                #the processed keys are deleted at the stage of mergence
            print("number of file = {},{}".format(num_w_files,num_b_files))
            hdelete_keys(endpoint, file_keys)
            #objects = hlist_keys(endpoint,bucket_name)
            #print("the keys being deleted = {}".format(objects))

    return w_grad_sum/num_workers, b_grad_sum/num_workers


def reduce_epoch(endpoint, vector, merged_bucket, num_workers, worker_index, postfix):
    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    curr_epoch = int(postfix)
    # put object to ec, format of key: workerID_epoch
    key = "{}_{}".format(worker_index, postfix)
    hset_object(endpoint, merged_bucket, key, vector.tobytes())

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        candidate = []
        for worker_index in range(num_workers):
            candidate.append("{}_{}_{}".format(merged_bucket, worker_index, postfix))
        num_files = 0
        while num_files < num_workers:
            file_keys = []
            objects = hlist_keys(endpoint, candidate)
            if objects is not None:
                for key, value in objects.items():
                    file_keys.append(key)
                    bytes_data = np.fromstring(value, dtype=vec_dtype)
                    tmp_vec = bytes_data.reshape(vec_shape)
                    merged_vec += tmp_vec
                    num_files += 1
                # print("number of file = {}".format(num_files))
                # the processed keys are deleted at the stage of mergence
                hdelete_keys(endpoint, file_keys)
        # write the merged data back to EC
        hset_object(endpoint, merged_bucket, 'merged_' + postfix, merged_vec.tobytes())
        if curr_epoch >= 1:
            hdelete_keys(endpoint, ["merged_{}".format(curr_epoch-1)])
    else:
        merged_file_name = 'merged_' + postfix
        merged_data = hget_object_or_wait(endpoint, merged_bucket, merged_file_name, 0.01)
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


def reduce_batch(endpoint, vector, merged_bucket, num_workers, worker_index, postfix):
    # vector is supposed to be a 1-d numpy array
    vec_shape = vector.shape
    vec_dtype = vector.dtype
    merged_vec = np.zeros(vec_shape, dtype=vec_dtype)

    postfix_splits = postfix.split("_")
    curr_epoch = int(postfix_splits[0])
    curr_batch = int(postfix_splits[1])

    # put object to ec, format of key: workerID_epoch_batch
    key = "{}_{}".format(worker_index, postfix)
    hset_object(endpoint, merged_bucket, key, vector.tobytes())

    # the first worker read and aggregate the corresponding chunk
    if worker_index == 0:
        candidate = []
        for worker_index in range(num_workers):
            candidate.append("{}_{}_{}".format(merged_bucket, worker_index, postfix))
        num_files = 0
        while num_files < num_workers:
            file_keys = []
            objects = hlist_keys(endpoint, candidate)
            if objects is not None:
                for key, value in objects.items():
                    file_keys.append(key)
                    bytes_data = np.fromstring(value, dtype=vec_dtype)
                    tmp_vec = bytes_data.reshape(vec_shape)
                    merged_vec += tmp_vec
                    num_files += 1
                # print("number of file = {}".format(num_files))
                # the processed keys are deleted at the stage of mergence
                hdelete_keys(endpoint, file_keys)
        # write the merged data back to EC
        hset_object(endpoint, merged_bucket, 'merged_' + postfix, merged_vec.tobytes())
        delete_expired_merged(endpoint, merged_bucket, curr_epoch, curr_batch, "merged")
    else:
        merged_file_name = 'merged_' + postfix
        merged_data = hget_object_or_wait(endpoint, merged_bucket, merged_file_name, 0.01)
        merged_vec = np.frombuffer(merged_data, dtype=vec_dtype).reshape(vec_shape)

    return merged_vec


def delete_expired_merged(endpoint, bucket_name, cur_epoch, cur_batch, prefix="merged"):
    candidate = []
    for epoch in range(cur_epoch+1):
        for batch in range(cur_batch):
            candidate.append("{}}_{}_{}".format(prefix, epoch, batch))
    hdelete_keys(endpoint, candidate)


def reduce_scatter_epoch(endpoint, vector, merged_bucket, num_workers, myrank, postfix):
    # vector is supposed to be a 1-d numpy array
    num_all_values = vector.size
    num_values_per_worker = num_all_values // num_workers
    residue = num_all_values % num_workers
    curr_epoch = int(postfix)

    my_offset = (num_values_per_worker * myrank) + min(residue, myrank)
    my_length = num_values_per_worker + (1 if myrank < residue else 0)
    my_chunk = vector[my_offset: my_offset + my_length]

    # write partitioned vector to the shared memory, except the chunk charged by myself
    for i in range(num_workers):
        if i != myrank:
            offset = (num_values_per_worker * i) + min(residue, i)
            length = num_values_per_worker + (1 if i < residue else 0)
            # indicating the chunk number and which worker it comes from
            # format of key in tmp-bucket: chunkID_workerID_epoch
            key = "{}_{}".format(i, myrank)
            hset_object(endpoint, merged_bucket, key + '_' + postfix, vector[offset: offset + length].tobytes())

    # read and aggregate the corresponding chunk
    candidate = []
    for worker_index in range(num_workers):
        if worker_index != myrank:
            candidate.append("{}_{}_{}_{}".format(merged_bucket, myrank, worker_index, postfix))
    num_files = 0
    while num_files < num_workers - 1:
        objects = hlist_keys(endpoint, candidate)
        if objects is not None:
            file_keys = []
            for key, value in objects.items():
                file_keys.append(key)
                bytes_data = np.fromstring(value, dtype=vector.dtype)
                my_chunk = my_chunk + bytes_data
                num_files += 1
                num_files += 1
            # the processed keys are deleted at the stage of mergence
            hdelete_keys(endpoint, file_keys)

    # write the aggregated chunk back
    # key format in merged_bucket: chunkID_epoch
    hset_object(endpoint, merged_bucket, str(myrank) + '_' + postfix, my_chunk.tobytes())

    # read other aggregated chunks
    merged_value = {}
    merged_value[myrank] = my_chunk

    candidate = []
    for worker_index in range(num_workers):
        if worker_index != myrank:
            candidate.append("{}_{}_{}".format(merged_bucket, worker_index, postfix))
    num_merged_files = 0
    already_read = []
    while num_merged_files < num_workers - 1:
        objects = hlist_keys(endpoint, candidate)
        if objects is not None:
            for key, value in objects.items():
                bytes_data = np.frombuffer(value, dtype=vector.dtype)
                key_splits = key.split("_")
                merged_value[int(key_splits[1])] = bytes_data
                already_read.append(key)
                num_merged_files += 1

    # reconstruct the whole vector
    result = merged_value[0]
    for k in range(1, num_workers):
        result = np.concatenate((result, merged_value[k]))

    return result


def put_merged_w_b_grads(endpoint, bucket_name, w_grad, b_grad, file_postfix,
                         w_grad_prefix="w_grad_", b_grad_prefix="b_grad"):
    print('put merged weight {} to bucket {}'.format(w_grad_prefix+file_postfix, (bucket_name,)))
    hset_object(endpoint, bucket_name,w_grad_prefix+file_postfix, w_grad.tobytes())
    print('put merged bias {} to bucket {}'.format(b_grad_prefix+file_postfix, bucket_name))
    hset_object(endpoint, bucket_name,b_grad_prefix+file_postfix, b_grad.tobytes())


def get_merged_w_b_grads(endpoint, bucket_name, file_postfix,
                        dtype, w_shape, b_shape,
                        w_prefix="w_grad_", b_prefix="b_grad"):
    #print("get merged weight {} in bucket {}".format(w_prefix+file_postfix , bucket_name))
    w_grad = np.fromstring(hget_object_or_wait(endpoint, bucket_name, w_prefix + file_postfix, 0.00001), dtype).reshape(w_shape)
    #print('get merged bias {} in bucket {}'.format(b_prefix+file_postfix, bucket_name))
    b_grad = np.fromstring(hget_object_or_wait(endpoint, bucket_name, b_prefix + file_postfix, 0.00001), dtype).reshape(b_shape)

    return w_grad, b_grad


def delete_expired_w_b_grads(endpoint, bucket_name, cur_epoch, cur_batch, end,
                             w_prefix="w_grad_", b_prefix="b_grad"):
    w_expired = [w_prefix+str(cur_epoch)+""+str(cur_batch-1),w_prefix+str(cur_epoch-1)+""+str(end-1)]
    b_expired = [b_prefix+str(cur_epoch)+""+str(cur_batch-1),b_prefix+str(cur_epoch-1)+""+str(end-1)]
    hdelete_keys(endpoint, bucket_name, w_expired+b_expired)


def clear_bucket(endpoint):
    endpoint.flush_all()
    return True
