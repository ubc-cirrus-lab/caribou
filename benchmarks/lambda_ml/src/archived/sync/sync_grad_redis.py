import numpy as np
import pickle

from archived.elasticache import hlist_keys
from archived.elasticache import hget_object,hget_object_or_wait
from archived.elasticache import hset_object
from archived.elasticache.Redis.delete_keys import hdelete_keys


def merge_w_b_layers(endpoint, bucket_name, num_workers, prefix):
    #whatever merges w/b grads or model
    num_files = 0
    merged_value = []

    while num_files < num_workers:
        objects = hlist_keys(endpoint, bucket_name)
        if objects is not None:
            for obj in objects:
                file_key = bytes.decode(obj)
                data_bytes = hget_object(endpoint, bucket_name, file_key)
                data = pickle.loads(data_bytes)

                for i in range(len(data)):
                    if num_files == 0:
                        merged_value.append(np.zeros(data[i].shape, dtype=data[i].dtype))

                    merged_value[i] = merged_value[i] + data[i]

                num_files = num_files + 1
                hdelete_keys(endpoint, bucket_name, [file_key])

    # average weights
    if prefix == 'w_':
        merged_value = [value / float(num_workers) for value in merged_value]

    return merged_value


def put_merged_w_b_layers(endpoint, bucket_name, merged_value, prefix, file_postfix):
    # print('put merged weight {} to bucket {}'.format(w_prefix + file_postfix, bucket_name))
    hset_object(endpoint, bucket_name, prefix + file_postfix, pickle.dumps(merged_value))
    # print('put merged bias {} to bucket {}'.format(b_prefix + file_postfix, bucket_name))
    # put_object(bucket_name, b_prefix + file_postfix, b.tobytes())


def get_merged_w_b_layers(endpoint, bucket_name, prefix, file_postfix):
    # print('get merged weight {} in bucket {}'.format(w_prefix + file_postfix, bucket_name))
    merged_value = hget_object_or_wait(endpoint, bucket_name, prefix + file_postfix, 0.1).read()
    merged_value_np = pickle.loads(merged_value)
    # merged_value_np = np.frombuffer(merged_value, dtype=dtype).reshape(dshape)

    return merged_value_np


def merge_w_b_grads(endpoint, bucket_name, num_workers,
                    dtype, w_shape, b_shape,
                    w_grad_prefix="w_grad_", b_grad_prefix="b_grad_"):
    num_w_files = 0
    num_b_files = 0
    w_grad_sum = np.zeros(w_shape, dtype=dtype)
    b_grad_sum = np.zeros(b_shape, dtype=dtype)

    while num_w_files < num_workers or num_b_files < num_workers:

        objects = hlist_keys(endpoint,bucket_name)
        while objects is not None:
            for obj in objects:
                file_key = bytes.decode(obj)
                print("the name of the file being processed = {}".format(file_key))
                bytes_data = np.fromstring(hget_object(endpoint, bucket_name, file_key), dtype)
                if file_key.startswith(w_grad_prefix):
                    w_grad = bytes_data.reshape(w_shape)
                    #print("merge the {}-th weight grad {} in bucket {} = {}".format(num_w_files, file_key, bucket_name, w_grad[0][:5]))
                    w_grad_sum = w_grad_sum + w_grad
                    num_w_files = num_w_files + 1
                elif file_key.startswith(b_grad_prefix):
                    b_grad = bytes_data.reshape(b_shape)
                    #print("merge the {}-th bias grad {} in bucket {} = {}".format(num_b_files, file_key, bucket_name, b_grad))
                    b_grad_sum = b_grad_sum + b_grad
                    num_b_files = num_b_files + 1

                hdelete_keys(endpoint,bucket_name,[file_key])
            objects = hlist_keys(endpoint,bucket_name)
            #print("the keys being deleted = {}".format(objects))

    return w_grad_sum/num_workers, b_grad_sum/num_workers


def put_merged_w_b_grads(endpoint, bucket_name, w_grad, b_grad,file_postfix,
                        w_grad_prefix="w_grad_", b_grad_prefix="b_grad_"):
    print('put merged weight {} to bucket {}'.format(w_grad_prefix+file_postfix, (bucket_name,)))
    hset_object(endpoint, bucket_name,w_grad_prefix+file_postfix, w_grad.tobytes())
    print('put merged bias {} to bucket {}'.format(b_grad_prefix+file_postfix, bucket_name))
    hset_object(endpoint, bucket_name,b_grad_prefix+file_postfix, b_grad.tobytes())


def get_merged_w_b_grads(endpoint, bucket_name, file_postfix,
                        dtype, w_shape, b_shape,
                        w_prefix="w_grad_", b_prefix="b_grad_"):
    print("get merged weight {} in bucket {}".format(w_prefix+file_postfix , bucket_name))
    w_grad = np.fromstring(hget_object_or_wait(endpoint, bucket_name,  w_prefix + file_postfix , 0.00001), dtype).reshape(w_shape)
    print('get merged bias {} in bucket {}'.format(b_prefix+file_postfix, bucket_name))
    b_grad = np.fromstring(hget_object_or_wait(endpoint, bucket_name, b_prefix + file_postfix, 0.00001), dtype).reshape(b_shape)

    return w_grad, b_grad


def delete_expired_w_b_grads(endpoint, bucket_name, cur_epoch, cur_batch,
                             w_prefix="w_grad_", b_prefix="b_grad_"):
    objects = hlist_keys(endpoint, bucket_name)
    if objects is not None:
        for obj in objects:
            file_key = bytes.decode(obj)
            if file_key.startswith(w_prefix) or file_key.startswith(b_prefix):
                key_splits = file_key.split("_")
                key_batch = int(key_splits[-1])
                key_epoch = int(key_splits[-2])
                if key_epoch < cur_epoch or (key_epoch == cur_epoch and key_batch < cur_batch):
                    print("delete object {} in bucket {}".format(file_key, bucket_name))
                    hdelete_keys(endpoint, bucket_name, [file_key])
    #does it like delete twice? because obviously I have delete all of it when I do the model average.


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


def clear_bucket(endpoint, bucket_name):

    objects = hlist_keys(endpoint, bucket_name)
    while objects!=None:
        if len(objects) >= 1:
            print("delete files {} in bucket {}".format(objects, bucket_name))
            hdelete_keys(endpoint, bucket_name, objects)
        objects = hlist_keys(endpoint, bucket_name)
    return True
