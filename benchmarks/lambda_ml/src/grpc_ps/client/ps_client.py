import time

from grpc_ps import ps_service_pb2
from google.protobuf import empty_pb2


def ping(grpc_client):
    response = grpc_client.ping(empty_pb2.Empty())
    if response.status is not True:
        raise Exception('ping PS failed')


def register_model(grpc_client, mid, n_worker, worker_index, length):
    if worker_index == 0:
        response = grpc_client.register_model(
            ps_service_pb2.RegisterRequest(id=mid, length=length, parallelism=n_worker))
        if response.status is not True:
            raise Exception('register model failed')


def exist_model(grpc_client, mid):
    response = grpc_client.exist_model(ps_service_pb2.ExistRequest(id=mid))
    is_model_exist = response.status
    while is_model_exist is not True:
        time.sleep(0.1)
        response = grpc_client.exist_model(ps_service_pb2.ExistRequest(id=mid))
        is_model_exist = response.status

    return True


def can_pull(grpc_client, mid, n_iter, worker_index):
    response = grpc_client.can_pull(
        ps_service_pb2.WorkerRequest(id=mid, n_iter=n_iter, worker_id=worker_index))
    flag = response.status
    while flag is not True:
        time.sleep(0.1)
        response = grpc_client.can_pull(
            ps_service_pb2.WorkerRequest(id=mid, n_iter=n_iter, worker_id=worker_index))
        flag = response.status
    return flag


def can_push(grpc_client, mid, n_iter, worker_index):
    response = grpc_client.can_push(
        ps_service_pb2.WorkerRequest(id=mid, n_iter=n_iter, worker_id=worker_index))
    flag = response.status
    while flag is not True:
        time.sleep(0.1)
        response = grpc_client.can_push(
            ps_service_pb2.WorkerRequest(id=mid, n_iter=n_iter, worker_id=worker_index))
        flag = response.status
    return flag


def pull_model(grpc_client, mid, n_iter, worker_index):
    response = grpc_client.pull_model(
        ps_service_pb2.WorkerRequest(id=mid, n_iter=n_iter, worker_id=worker_index))
    return response.data


def push_grad(grpc_client, mid, np_1d_arr, learning_rate, n_iter, worker_index):
    grad = ps_service_pb2.Grad()
    grad.id = mid
    grad.learning_rate = learning_rate
    grad_list = np_1d_arr.tolist()
    grad.data.extend(grad_list)
    grad.length = len(grad_list)
    grad.n_iter = n_iter
    grad.worker_id = worker_index
    response = grpc_client.push_grad(grad)
    if response.status is not True:
        raise Exception('push grad failed')


def push_update(grpc_client, mid, np_1d_arr, n_iter, worker_index):
    update = ps_service_pb2.Update()
    update.id = mid
    update_list = np_1d_arr.tolist()
    update.data.extend(update_list)
    update.length = len(update_list)
    update.n_iter = n_iter
    update.worker_id = worker_index
    response = grpc_client.push_update(update)
    if response.status is not True:
        raise Exception('push update failed')
