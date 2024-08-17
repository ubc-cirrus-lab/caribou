import time
import sys
sys.path.append("../../")

from thrift_ps.ps_service import ParameterServer
from thrift_ps.ps_service.ttypes import Model, Update, Grad, InvalidOperation

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from thrift_ps import constants

import argparse


def ping(thrift_client):
    thrift_client.ping()


def register_model(thrift_client, worker_index, mid, length, n_worker):
    if worker_index == 0:
        thrift_client.register_model(mid, length, n_worker)


def exist_model(thrift_client, mid):
    is_model_exist = thrift_client.exist_model(mid)
    while is_model_exist is not True:
        time.sleep(0.1)
        is_model_exist = thrift_client.exist_model(mid)
    return True


def can_pull(thrift_client, mid, n_iter, worker_index):
    flag = thrift_client.can_pull(mid, n_iter, worker_index)
    while flag is not True:
        time.sleep(0.1)
        flag = thrift_client.can_pull(mid, n_iter, worker_index)
    return flag


def can_push(thrift_client, mid, n_iter, worker_index):
    flag = thrift_client.can_push(mid, n_iter, worker_index)
    while flag is not True:
        time.sleep(0.1)
        flag = thrift_client.can_push(mid, n_iter, worker_index)
    return flag


def pull_model(thrift_client, mid, n_iter, worker_index):
    weight = thrift_client.pull_model(mid, n_iter, worker_index)
    return weight.data


def push_grad(thrift_client, mid, np_1d_arr, learning_rate, n_iter, worker_index):
    grad = Grad()
    grad.id = mid
    grad.learning_rate = learning_rate
    grad.data = np_1d_arr.tolist()
    grad.length = len(grad.data)
    grad.n_iter = n_iter
    grad.worker_id = worker_index
    thrift_client.push_grad(grad)


def push_update(thrift_client, mid, np_1d_arr, learning_rate, n_iter, worker_index):
    update = Update()
    update.id = mid
    update.data = np_1d_arr.tolist()
    update.length = len(update.data)
    update.n_iter = n_iter
    update.worker_id = worker_index
    thrift_client.push_update(update)


MODEL_ID = "weight"
MODEL_LENGTH = 10
N_WORKER = 2


# python ps_client.py --rank 0 --host spaceml1 --port 27000

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rank', type=int, default=0)
    parser.add_argument('--host', type=str, default=constants.HOST)
    parser.add_argument('--port', type=int, default=constants.PORT)
    args = parser.parse_args()
    print(args)

    worker_index = args.rank
    # Make socket
    transport = TSocket.TSocket(args.host, args.port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = ParameterServer.Client(protocol)
    # Connect!
    transport.open()

    client.ping()
    print('ping()')

    # register model
    if worker_index == 0:
        client.register_model(MODEL_ID, MODEL_LENGTH, N_WORKER)

    is_model_exist = False
    while is_model_exist is not True:
        is_model_exist = client.exist_model(MODEL_ID)

    # pull latest model
    try:
        can_pull = False
        while can_pull is not True:
            can_pull = client.can_pull(MODEL_ID, 0, worker_index)
        weight = client.pull_model(MODEL_ID, 0, worker_index)
        weight_data = weight.data
        print("current weight = {}".format(weight_data))
    except InvalidOperation as e:
        print('InvalidOperation: %r' % e)

    # push gradient
    try:
        can_push = False
        while can_push is not True:
            can_push = client.can_push(MODEL_ID, 0, worker_index)
        grad = Grad()
        grad.id = MODEL_ID
        grad.learning_rate = 0.01
        grad.length = 10
        grad.data = [i for i in range(MODEL_LENGTH)]
        grad.n_iter = 0
        grad.worker_id = worker_index
        client.push_grad(grad)
    except InvalidOperation as e:
        print('InvalidOperation: %r' % e)

    # get latest model
    try:
        can_pull = False
        while can_pull is not True:
            can_pull = client.can_pull(MODEL_ID, 1, worker_index)
        weight = client.pull_model(MODEL_ID, 1, worker_index)
        weight_data = weight.data
        print("current weight = {}".format(weight_data))
    except InvalidOperation as e:
        print('InvalidOperation: %r' % e)

    # push update
    try:
        can_push = False
        while can_push is not True:
            can_push = client.can_push(MODEL_ID, 1, worker_index)
        update = Update()
        update.id = MODEL_ID
        update.length = MODEL_LENGTH
        update.data = [i for i in range(MODEL_LENGTH)]
        update.n_iter = 1
        update.worker_id = worker_index
        client.push_update(update)
    except InvalidOperation as e:
        print('InvalidOperation: %r' % e)

    # get latest model
    try:
        can_pull = False
        while can_pull is not True:
            can_pull = client.can_pull(MODEL_ID, 2, worker_index)
        weight = client.pull_model(MODEL_ID, 2, worker_index)
        weight_data = weight.data
        print("current weight = {}".format(weight_data))
    except InvalidOperation as e:
        print('InvalidOperation: %r' % e)

    # Close!
    transport.close()


if __name__ == "__main__":
    main()
