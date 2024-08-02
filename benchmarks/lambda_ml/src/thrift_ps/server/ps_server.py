import sys
import threading
import time

import numpy as np

from thrift_ps.ps_service import ParameterServer
from thrift_ps.ps_service.ttypes import Model, Grad, Update, Operation, InvalidOperation


class PSHandler:

    def __init__(self):
        self.models = {}
        self.model_ts = {}
        self.model_parallelism = {}
        self.model_pull_count = {}
        self.model_push_count = {}
        self.w_lock = threading.Lock()

    def model_ids(self):
        return self.model_ts.keys()

    def delete_expired(self, lower):
        print("current model on PS >>> {}".format(self.models.keys()))
        print("current model parallelism on PS >>> {}".format(self.model_parallelism))
        print("current model pull count on PS >>> {}".format(self.model_pull_count))
        print("current model push count on PS >>> {}".format(self.model_push_count))
        for k, v in self.model_ts.items():
            if v <= lower:
                self.delete(k)

    def delete(self, mid):
        print("delete model {}".format(mid))
        self.models.pop(mid)
        self.model_ts.pop(mid)
        self.model_parallelism.pop(mid)
        self.model_pull_count.pop(mid)
        self.model_push_count.pop(mid)

    def ping(self):
        print('ping()')

    def register_model(self, mid, length, parallelism):
        self.models[mid] = 0.2 * np.random.rand(length) - 0.1
        self.model_ts[mid] = time.time()
        self.model_parallelism[mid] = parallelism
        self.model_pull_count[mid] = 0
        self.model_push_count[mid] = 0
        print("register model >>> id = {}, length = {}, parallelism = {}"
              .format(mid, length, parallelism))

    def exist_model(self, mid):
        return self.models.__contains__(mid)

    def can_pull(self, mid, n_iter, worker_id):
        #print("worker {} ask can_pull >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_push_count.__contains__(mid):
            return self.model_push_count[mid] == self.model_parallelism[mid] * n_iter
        else:
            x = InvalidOperation()
            x.whatOp = Operation.CAN_PULL
            x.why = 'No model {} in model_push_count on PS'.format(mid)
            raise x

    def can_push(self, mid, n_iter, worker_id):
        #print("worker {} ask can_push >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_pull_count.__contains__(mid):
            return self.model_pull_count[mid] == self.model_parallelism[mid] * (n_iter+1)
        else:
            x = InvalidOperation()
            x.whatOp = Operation.CAN_PUSH
            x.why = 'No model {} in model_pull_count on PS'.format(mid)
            raise x

    def pull_model(self, mid, n_iter, worker_id):
        print("worker {} pull model >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.models.__contains__(mid):
            model = Model()
            model.id = mid
            model.data = self.models[mid].tolist()
            model.length = len(model.data)
            self.model_pull_count[mid] = self.model_pull_count[mid] + 1
            return model
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PULL_MODEL
            x.why = 'No model {} on PS'.format(mid)
            raise x

    def push_grad(self, grad):
        print("worker {} push grad >>> id = {}, lr = {}, n_iter = {}"
              .format(grad.worker_id, grad.id, grad.learning_rate, grad.n_iter))
        update_start = time.time()
        if self.models.__contains__(grad.id):
            self.w_lock.acquire()
            self.models[grad.id] = np.add(self.models.get(grad.id),
                                          np.multiply(grad.data, grad.learning_rate))
            self.model_push_count[grad.id] = self.model_push_count[grad.id] + 1
            self.w_lock.release()
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PUSH_GRAD
            x.why = 'No model {} on PS'.format(grad.id)
            raise x
        print("update model cost {} s".format(time.time() - update_start))

    def push_update(self, update):
        print("worker {} push update >>> id = {}, n_iter = {}"
              .format(update.worker_id, update.id, update.n_iter))
        update_start = time.time()
        if self.models.__contains__(update.id):
            self.w_lock.acquire()
            self.models[update.id] = np.add(self.models.get(update.id), update.data)
            self.model_push_count[update.id] = self.model_push_count[update.id] + 1
            self.w_lock.release()
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PUSH_UPDATE
            x.why = 'No model {} on PS'.format(update.id)
            raise x
        print("update model cost {} s".format(time.time() - update_start))


# store grad to file, and merge them
class PSHandler2:

    def __init__(self, tmp_dir):
        self.models = {}
        self.model_ts = {}
        self.model_ind = {}  # locate model in an array
        self.num_model = 0
        self.model_parallelism = {}
        self.model_pull_count = {}
        self.model_push_count = {}
        self.tmp_dir = tmp_dir
        self.num_file = []
        self.w_lock = threading.Lock()

    def model_ids(self):
        return self.model_ts.keys()

    def delete_expired(self, lower):
        print("current model on PS >>> {}".format(self.models.keys()))
        print("current model parallelism on PS >>> {}".format(self.model_parallelism))
        print("current model pull count on PS >>> {}".format(self.model_pull_count))
        print("current model push count on PS >>> {}".format(self.model_push_count))
        for k, v in self.model_ts.items():
            if v <= lower:
                self.delete(k)

    def delete(self, mid):
        print("delete model {}".format(mid))
        self.models.pop(mid)
        self.model_ts.pop(mid)
        self.model_ind.pop(mid)
        self.model_parallelism.pop(mid)
        self.model_pull_count.pop(mid)
        self.model_push_count.pop(mid)

    def ping(self):
        print('ping()')

    def register_model(self, mid, length, parallelism):
        self.models[mid] = np.random.rand(length)
        self.model_ts[mid] = time.time()
        self.model_ind[mid] = self.num_model
        self.num_model += 1
        self.num_file.append(0)
        self.model_parallelism[mid] = parallelism
        self.model_pull_count[mid] = 0
        self.model_push_count[mid] = 0
        print("register model >>> id = {}, length = {}, parallelism = {}"
              .format(mid, length, parallelism))

    def exist_model(self, mid):
        return self.models.__contains__(mid)

    def inc_num_file(self, mid):
        ind = self.model_ind[mid]
        self.num_file[ind] += 1

    def can_merge(self, mid):
        ind = self.model_ind[mid]
        n_file = self.num_file[ind]
        return n_file == self.model_parallelism[mid]

    def merge_grad(self, mid, lr):
        for i in range(self.model_parallelism[mid]):
            tmp_name = "{}/{}_{}_{}.npy".format(self.tmp_dir, mid, i, self.model_parallelism[mid])
            tmp_arr = np.load(tmp_name)
            self.models[mid] = np.add(self.models.get(mid), np.multiply(tmp_arr, lr))
        self.reset_num_file(mid)

    def merge_update(self, mid):
        for i in range(self.model_parallelism[mid]):
            tmp_name = "{}/{}_{}_{}.npy".format(self.tmp_dir, mid, i, self.model_parallelism[mid])
            tmp_arr = np.load(tmp_name)
            self.models[mid] = np.add(self.models.get(mid), tmp_arr)
        self.reset_num_file(mid)

    def reset_num_file(self, mid):
        ind = self.model_ind[mid]
        self.num_file[ind] = 0

    def can_pull(self, mid, n_iter, worker_id):
        #print("worker {} ask can_pull >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_push_count.__contains__(mid):
            return self.model_push_count[mid] == self.model_parallelism[mid] * n_iter
        else:
            x = InvalidOperation()
            x.whatOp = Operation.CAN_PULL
            x.why = 'No model {} in model_push_count on PS'.format(mid)
            raise x

    def can_push(self, mid, n_iter, worker_id):
        #print("worker {} ask can_push >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_pull_count.__contains__(mid):
            return self.model_pull_count[mid] == self.model_parallelism[mid] * (n_iter+1)
        else:
            x = InvalidOperation()
            x.whatOp = Operation.CAN_PUSH
            x.why = 'No model {} in model_pull_count on PS'.format(mid)
            raise x

    def pull_model(self, mid, n_iter, worker_id):
        print("worker {} pull model >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.models.__contains__(mid):
            model = Model()
            model.id = mid
            model.data = self.models[mid].tolist()
            model.length = len(model.data)
            self.model_pull_count[mid] = self.model_pull_count[mid] + 1
            return model
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PULL_MODEL
            x.why = 'No model {} on PS'.format(mid)
            raise x

    def push_grad(self, grad):
        print("worker {} push grad >>> id = {}, lr = {}, n_iter = {}"
              .format(grad.worker_id, grad.id, grad.learning_rate, grad.n_iter))
        if self.models.__contains__(grad.id):
            save_start = time.time()
            f_name = "{}/{}_{}_{}.npy".format(self.tmp_dir, grad.id, grad.worker_id, self.model_parallelism[grad.id])
            np.save(f_name, np.array(grad.data))
            print("save file {}, cost {} s".format(f_name, time.time() - save_start))
            self.inc_num_file(grad.id)
            if self.can_merge(grad.id):
                merge_start = time.time()
                self.merge_grad(grad.id, grad.learning_rate)
                print("merge cost {} s".format(time.time() - merge_start))
            self.w_lock.acquire()
            self.model_push_count[grad.id] = self.model_push_count[grad.id] + 1
            self.w_lock.release()
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PUSH_GRAD
            x.why = 'No model {} on PS'.format(grad.id)
            raise x

    def push_update(self, update):
        print("worker {} push update >>> id = {}, n_iter = {}"
              .format(update.worker_id, update.id, update.n_iter))
        if self.models.__contains__(update.id):
            save_start = time.time()
            f_name = "{}/{}_{}_{}.npy".format(self.tmp_dir, update.id, update.worker_id, self.model_parallelism[update.id])
            np.save(f_name, np.array(update.data))
            print("save file {}, cost {} s".format(f_name, time.time() - save_start))
            self.inc_num_file(update.id)
            if self.can_merge(update.id):
                merge_start = time.time()
                self.merge_update(update.id)
                print("merge cost {} s".format(time.time() - merge_start))
            self.w_lock.acquire()
            self.model_push_count[update.id] = self.model_push_count[update.id] + 1
            self.w_lock.release()
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PUSH_UPDATE
            x.why = 'No model {} on PS'.format(update.id)
            raise x


# store grad in memory
class PSHandler3:

    def __init__(self, tmp_dir):
        self.models = {}
        self.model_ts = {}
        self.model_ind = {}  # locate model in an array
        self.num_model = 0
        self.model_parallelism = {}
        self.model_pull_count = {}
        self.model_push_count = {}
        self.tmp_data = []
        self.num_data = []
        self.w_lock = threading.Lock()

    def model_ids(self):
        return self.model_ts.keys()

    def delete_expired(self, lower):
        print("current model on PS >>> {}".format(self.models.keys()))
        print("current model parallelism on PS >>> {}".format(self.model_parallelism))
        print("current model pull count on PS >>> {}".format(self.model_pull_count))
        print("current model push count on PS >>> {}".format(self.model_push_count))
        for k, v in self.model_ts.items():
            if v <= lower:
                self.delete(k)

    def delete(self, mid):
        print("delete model {}".format(mid))
        self.models.pop(mid)
        self.model_ts.pop(mid)
        self.model_ind.pop(mid)
        self.model_parallelism.pop(mid)
        self.model_pull_count.pop(mid)
        self.model_push_count.pop(mid)

    def ping(self):
        print('ping()')

    def register_model(self, mid, length, parallelism):
        self.models[mid] = np.random.rand(length)
        self.model_ts[mid] = time.time()
        self.model_ind[mid] = self.num_model
        self.num_model += 1
        self.num_data.append(0)
        self.model_parallelism[mid] = parallelism
        self.model_pull_count[mid] = 0
        self.model_push_count[mid] = 0
        print("register model >>> id = {}, length = {}, parallelism = {}"
              .format(mid, length, parallelism))

    def exist_model(self, mid):
        return self.models.__contains__(mid)

    def inc_num_data(self, mid):
        ind = self.model_ind[mid]
        self.num_data[ind] += 1

    def can_merge(self, mid):
        ind = self.model_ind[mid]
        n_file = self.num_data[ind]
        return n_file == self.model_parallelism[mid]

    def merge_grad(self, mid, lr):
        for i in range(self.model_parallelism[mid]):
            tmp_arr = self.tmp_data[i]
            self.models[mid] = np.add(self.models.get(mid), np.multiply(tmp_arr, lr))
        self.reset_num_data(mid)

    def merge_update(self, mid):
        for i in range(self.model_parallelism[mid]):
            tmp_arr = self.tmp_data[i]
            self.models[mid] = np.add(self.models.get(mid), tmp_arr)
        self.reset_num_data(mid)

    def reset_num_data(self, mid):
        ind = self.model_ind[mid]
        self.num_data[ind] = 0
        self.tmp_data.clear()

    def can_pull(self, mid, n_iter, worker_id):
        #print("worker {} ask can_pull >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_push_count.__contains__(mid):
            return self.model_push_count[mid] == self.model_parallelism[mid] * n_iter
        else:
            x = InvalidOperation()
            x.whatOp = Operation.CAN_PULL
            x.why = 'No model {} in model_push_count on PS'.format(mid)
            raise x

    def can_push(self, mid, n_iter, worker_id):
        #print("worker {} ask can_push >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_pull_count.__contains__(mid):
            return self.model_pull_count[mid] == self.model_parallelism[mid] * (n_iter+1)
        else:
            x = InvalidOperation()
            x.whatOp = Operation.CAN_PUSH
            x.why = 'No model {} in model_pull_count on PS'.format(mid)
            raise x

    def pull_model(self, mid, n_iter, worker_id):
        print("worker {} pull model >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.models.__contains__(mid):
            model = Model()
            model.id = mid
            model.data = self.models[mid].tolist()
            model.length = len(model.data)
            self.model_pull_count[mid] = self.model_pull_count[mid] + 1
            return model
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PULL_MODEL
            x.why = 'No model {} on PS'.format(mid)
            raise x

    def push_grad(self, grad):
        print("worker {} push grad >>> id = {}, lr = {}, n_iter = {}"
              .format(grad.worker_id, grad.id, grad.learning_rate, grad.n_iter))
        if self.models.__contains__(grad.id):
            self.tmp_data.append(np.array(grad.data))
            self.inc_num_data(grad.id)
            if self.can_merge(grad.id):
                self.merge_grad(grad.id, grad.learning_rate)
            self.w_lock.acquire()
            self.model_push_count[grad.id] = self.model_push_count[grad.id] + 1
            self.w_lock.release()
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PUSH_GRAD
            x.why = 'No model {} on PS'.format(grad.id)
            raise x

    def push_update(self, update):
        print("worker {} push update >>> id = {}, n_iter = {}"
              .format(update.worker_id, update.id, update.n_iter))
        if self.models.__contains__(update.id):
            self.tmp_data.append(np.array(update.data))
            self.inc_num_data(update.id)
            if self.can_merge(update.id):
                self.merge_update(update.id)
            self.w_lock.acquire()
            self.model_push_count[update.id] = self.model_push_count[update.id] + 1
            self.w_lock.release()
        else:
            x = InvalidOperation()
            x.whatOp = Operation.PUSH_UPDATE
            x.why = 'No model {} on PS'.format(update.id)
            raise x