import sys
import threading
import time

import numpy as np

import sys
sys.path.append("../../")
from grpc_ps import ps_service_pb2
from grpc_ps import ps_service_pb2_grpc


class PSHandler(ps_service_pb2_grpc.ParameterServerServicer):

    def __init__(self):
        self.models = {}
        self.model_ts = {}
        self.model_parallelism = {}
        self.model_pull_count = {}
        self.model_push_count = {}
        self.w_lock = threading.Lock()

    def model_ids(self):
        return self.model_ts.keys()

    def delete(self, mid):
        print("delete model {}".format(mid))
        self.models.pop(mid)
        self.model_ts.pop(mid)
        self.model_parallelism.pop(mid)
        self.model_pull_count.pop(mid)
        self.model_push_count.pop(mid)

    def ping(self, request, context):
        response = ps_service_pb2.Status(
            status=True
        )
        return response

    def register_model(self, request, context):
        mid = request.id
        length = request.length
        parallelism = request.parallelism
        self.models[mid] = np.random.rand(length)
        self.model_ts[mid] = time.time()
        self.model_parallelism[mid] = parallelism
        self.model_pull_count[mid] = 0
        self.model_push_count[mid] = 0
        print("register model >>> id = {}, length = {}, parallelism = {}"
              .format(mid, length, parallelism))
        response = ps_service_pb2.Status(status=True)
        return response

    def exist_model(self, request, context):
        mid = request.id
        response = ps_service_pb2.Status(
            status=self.models.__contains__(mid)
        )
        return response

    def can_pull(self, request, context):
        mid = request.id
        n_iter = request.n_iter
        worker_id = request.worker_id
        #print("worker {} ask can_pull >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_push_count.__contains__(mid):
            return ps_service_pb2.Status(
                status=self.model_push_count[mid] == self.model_parallelism[mid] * n_iter
            )
        else:
            print('No model {} in model_push_count on PS'.format(mid))
            return ps_service_pb2.Status(status=False)

    def can_push(self, request, context):
        mid = request.id
        n_iter = request.n_iter
        worker_id = request.worker_id
        #print("worker {} ask can_push >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        if self.model_pull_count.__contains__(mid):
            return ps_service_pb2.Status(
                status=self.model_pull_count[mid] == self.model_parallelism[mid] * (n_iter+1)
            )
        else:
            print('No model {} in model_pull_count on PS'.format(mid))
            return ps_service_pb2.Status(status=False)

    def pull_model(self, request, context):
        mid = request.id
        n_iter = request.n_iter
        worker_id = request.worker_id
        print("worker {} pull model >>> id = {}, iter = {}".format(worker_id, mid, n_iter))
        data = self.models[mid].tolist()
        length = len(data)
        response = ps_service_pb2.Model()
        response.id = mid
        response.length = length
        response.data.extend(data)
        self.model_pull_count[mid] = self.model_pull_count[mid] + 1
        return response

    def push_grad(self, request, context):
        mid = request.id
        learning_rate = request.learning_rate
        length = request.length
        data = request.data
        n_iter = request.n_iter
        worker_id = request.worker_id
        print("worker {} push grad >>> id = {}, lr = {}, n_iter = {}"
              .format(worker_id, mid, learning_rate, n_iter))
        update_start = time.time()
        if self.models.__contains__(mid):
            self.w_lock.acquire()
            self.models[mid] = np.add(self.models.get(mid), np.multiply(data, learning_rate))
            self.model_push_count[mid] = self.model_push_count[mid] + 1
            self.w_lock.release()
            print("update model cost {} s".format(time.time() - update_start))
            return ps_service_pb2.Status(status=True)
        else:
            print('No model {} on PS'.format(mid))
            return ps_service_pb2.Status(status=False)

    def push_update(self, request, context):
        mid = request.id
        length = request.length
        data = request.data
        n_iter = request.n_iter
        worker_id = request.worker_id
        print("worker {} push update >>> id = {}, n_iter = {}".format(worker_id, mid, n_iter))
        update_start = time.time()
        if self.models.__contains__(mid):
            self.w_lock.acquire()
            self.models[mid] = np.add(self.models.get(mid), data)
            self.model_push_count[mid] = self.model_push_count[mid] + 1
            self.w_lock.release()
            print("update model cost {} s".format(time.time() - update_start))
            return ps_service_pb2.Status(status=True)
        else:
            print('No model {} on PS'.format(mid))
            return ps_service_pb2.Status(status=False)
