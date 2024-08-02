import urllib
import numpy as np
import pickle

from src.storage import DynamoTable


def async_reduce(table, weight_bytes, key_col, key):
    assert isinstance(table, DynamoTable)

    table.save(weight_bytes, key, key_col)

    new_weight_bytes = table.load_or_wait(key, key_col, 0.1)['value'].value
    new_weight = pickle.loads(new_weight_bytes)

    return new_weight


def reduce_batch(tmp_table, merged_table, weight_bytes, key_col, n_workers, worker_index, cur_epoch, cur_batch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # put object to tmp table, format of key: workerID_epoch_batch
    my_key = "{}_{}".format(cur_epoch, cur_batch)
    tmp_table.save(weight_bytes, "{}_{}".format(worker_index, my_key), key_col)

    merged_value = []

    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            items = tmp_table.list()
            if items is not None and len(items) > 0:
                delete_list = []
                for item in items:
                    tmp_key = item[key_col]
                    key_splits = tmp_key.split("_")
                    key_epoch = key_splits[-2]
                    key_batch = key_splits[-1]
                    if key_epoch == str(cur_epoch) and key_batch == str(cur_batch):
                        bytes_data = item['value'].value
                        pickle_data = pickle.loads(bytes_data)
                        for i in range(len(pickle_data)):
                            if n_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        n_files = n_files + 1
                        delete_list.append(tmp_key)
                tmp_table.delete(delete_list, key_col)
        # average gradients
        merged_value = [value / float(n_workers) for value in merged_value]
        # write the merged data back to merged table
        merged_key = 'merged_{}'.format(my_key)
        merged_table.save(pickle.dumps(merged_value), merged_key, key_col)
        delete_expired_batch(merged_table, key_col, cur_epoch, cur_batch)

    # read merged data
    merged_key = 'merged_{}'.format(my_key)
    merged_data_bytes = merged_table.load_or_wait(merged_key, key_col, 0.1)['value'].value
    merged_weight = pickle.loads(merged_data_bytes)

    return merged_weight


def reduce_epoch(tmp_table, merged_table, weight_bytes, key_col, n_workers, worker_index, cur_epoch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # put object to tmp table, format of key: workerID_epoch_batch
    my_key = str(cur_epoch)
    tmp_table.save(weight_bytes, "{}_{}".format(worker_index, my_key), key_col)

    merged_value = []

    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            items = tmp_table.list()
            if items is not None and len(items) > 0:
                delete_list = []
                for item in items:
                    tmp_key = item[key_col]
                    key_splits = tmp_key.split("_")
                    key_epoch = key_splits[-1]
                    if key_epoch == str(cur_epoch):
                        bytes_data = item['value'].value
                        pickle_data = pickle.loads(bytes_data)
                        for i in range(len(pickle_data)):
                            if n_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        n_files = n_files + 1
                        delete_list.append(tmp_key)
                tmp_table.delete(delete_list, key_col)
        # average weights
        merged_value = [value / float(n_workers) for value in merged_value]
        # write the merged data back to merged table
        merged_key = 'merged_{}'.format(my_key)
        merged_table.save(pickle.dumps(merged_value), merged_key, key_col)
        delete_expired_epoch(merged_table, key_col, cur_epoch)

    # read merged data
    merged_key = 'merged_{}'.format(my_key)
    merged_data_bytes = merged_table.load_or_wait(merged_key, key_col, 0.1)['value'].value
    merged_weight = pickle.loads(merged_data_bytes)

    return merged_weight


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


def reduce_scatter_batch(tmp_table, merged_table, weight_bytes, key_col, n_workers, worker_index, cur_epoch, cur_batch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # put object to tmp table, format of key: workerID_epoch_batch
    my_key = "{}_{}".format(cur_epoch, cur_batch)
    tmp_table.save(weight_bytes, "{}_{}".format(worker_index, my_key), key_col)

    merged_value = []

    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            items = tmp_table.list()
            if items is not None and len(items) > 0:
                delete_list = []
                for item in items:
                    tmp_key = item[key_col]
                    key_splits = tmp_key.split("_")
                    key_epoch = key_splits[-2]
                    key_batch = key_splits[-1]
                    if key_epoch == str(cur_epoch) and key_batch == str(cur_batch):
                        bytes_data = item['value'].value
                        pickle_data = pickle.loads(bytes_data)
                        for i in range(len(pickle_data)):
                            if n_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        n_files = n_files + 1
                        delete_list.append(tmp_key)
                tmp_table.delete(delete_list, key_col)
        # average gradients
        merged_value = [value / float(n_workers) for value in merged_value]
        # write the merged data back to merged table
        merged_key = 'merged_{}'.format(my_key)
        merged_table.save(pickle.dumps(merged_value), merged_key, key_col)
        delete_expired_batch(merged_table, key_col, cur_epoch, cur_batch)

    # read merged data
    merged_key = 'merged_{}'.format(my_key)
    merged_data_bytes = merged_table.load_or_wait(merged_key, key_col, 0.1)['value'].value
    merged_weight = pickle.loads(merged_data_bytes)

    return merged_weight


def reduce_scatter_epoch(tmp_table, merged_table, weight_bytes, key_col, n_workers, worker_index, cur_epoch):
    assert isinstance(tmp_table, DynamoTable)
    assert isinstance(merged_table, DynamoTable)

    # put object to tmp table, format of key: workerID_epoch_batch
    my_key = str(cur_epoch)
    tmp_table.save(weight_bytes, "{}_{}".format(worker_index, my_key), key_col)

    merged_value = []

    if worker_index == 0:
        n_files = 0
        while n_files < n_workers:
            items = tmp_table.list()
            if items is not None and len(items) > 0:
                delete_list = []
                for item in items:
                    tmp_key = item[key_col]
                    key_splits = tmp_key.split("_")
                    key_epoch = key_splits[-1]
                    if key_epoch == str(cur_epoch):
                        bytes_data = item['value'].value
                        pickle_data = pickle.loads(bytes_data)
                        for i in range(len(pickle_data)):
                            if n_files == 0:
                                merged_value.append(np.zeros(pickle_data[i].shape, dtype=pickle_data[i].dtype))
                            merged_value[i] = merged_value[i] + pickle_data[i]
                        n_files = n_files + 1
                        delete_list.append(tmp_key)
                tmp_table.delete(delete_list, key_col)
        # average weights
        merged_value = [value / float(n_workers) for value in merged_value]
        # write the merged data back to merged table
        merged_key = 'merged_{}'.format(my_key)
        merged_table.save(pickle.dumps(merged_value), merged_key, key_col)
        delete_expired_epoch(merged_table, key_col, cur_epoch)

    # read merged data
    merged_key = 'merged_{}'.format(my_key)
    merged_data_bytes = merged_table.load_or_wait(merged_key, key_col, 0.1)['value'].value
    merged_weight = pickle.loads(merged_data_bytes)

    return merged_weight
