import time
import numpy as np
import argparse

import sys
sys.path.append("../../")
import grpc
from grpc_ps import ps_service_pb2_grpc
from grpc_ps.client import ps_client

# algorithm setting
NUM_EPOCHS = 10
NUM_BATCHES = 1
MODEL_NAME = "w.b"
LEARNING_RATE = 0.1


def handler(event, context):
    start_time = time.time()
    worker_index = event['rank']
    num_workers = event['num_workers']
    host = event['host']
    port = event['port']
    size = event['size']

    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print("host = {}".format(host))
    print("port = {}".format(port))
    print("size = {}".format(size))

    channel = grpc.insecure_channel("{}:{}".format(host, port), options=[
        ('grpc.max_send_message_length', 128 * 1024 * 1024),
        ('grpc.max_receive_message_length', 128 * 1024 * 1024)])
    stub = ps_service_pb2_grpc.ParameterServerStub(channel)
    # ping
    ps_client.ping(stub)
    print("create and ping thrift server >>> HOST = {}, PORT = {}".format(host, port))

    # register model
    ps_client.register_model(stub, MODEL_NAME, num_workers, worker_index, size)
    ps_client.exist_model(stub, MODEL_NAME)
    print("register and check model >>> name = {}, length = {}".format(MODEL_NAME, size))

    # Training the Model
    train_start = time.time()
    iter_counter = 0
    for epoch in range(NUM_EPOCHS):
        epoch_start = time.time()
        for batch_index in range(NUM_BATCHES):
            print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
            batch_start = time.time()

            loss = 0.0

            # pull latest model
            ps_client.can_pull(stub, MODEL_NAME, iter_counter, worker_index)
            pull_start = time.time()
            latest_model = ps_client.pull_model(stub, MODEL_NAME, iter_counter, worker_index)
            pull_time = time.time() - pull_start

            # push gradient to PS
            w_b_grad = np.random.rand(1, size).astype(np.double).flatten()
            ps_client.can_push(stub, MODEL_NAME, iter_counter, worker_index)
            push_start = time.time()
            ps_client.push_grad(stub, MODEL_NAME, w_b_grad, LEARNING_RATE, iter_counter, worker_index)
            push_time = time.time() - push_start
            ps_client.can_pull(stub, MODEL_NAME, iter_counter + 1, worker_index)  # sync all workers

            print('Epoch: [%d/%d], Step: [%d/%d] >>> Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: pull model cost %.4f s, push update cost %.4f s'
                  % (epoch + 1, NUM_EPOCHS, batch_index, NUM_BATCHES,
                     time.time() - train_start, loss, time.time() - epoch_start,
                     time.time() - batch_start, pull_time, push_time))
            iter_counter += 1

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
