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
NUM_BATCHES = 10
MODEL_NAME = "w.b"
LEARNING_RATE = 0.1

# python ps_client_test.py --num-workers 1 --rank 0 --host spaceml1 --port 27000 --size 100


def main():
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('--num-workers', type=int, default=1)
    parser.add_argument('--rank', type=int, default=0)
    parser.add_argument('--host', type=str, default="127.0.0.1")
    parser.add_argument('--port', type=int, default=27000)
    parser.add_argument('--size', type=int, default=100)
    args = parser.parse_args()
    print(args)

    channel = grpc.insecure_channel("{}:{}".format(args.host, args.port), options=[
        ('grpc.max_send_message_length', 128 * 1024 * 1024),
        ('grpc.max_receive_message_length', 128 * 1024 * 1024)])
    stub = ps_service_pb2_grpc.ParameterServerStub(channel)
    # ping
    ps_client.ping(stub)
    print("create and ping thrift server >>> HOST = {}, PORT = {}".format(args.host, args.port))

    # register model
    ps_client.register_model(stub, MODEL_NAME, args.num_workers, args.rank, args.size)
    ps_client.exist_model(stub, MODEL_NAME)
    print("register and check model >>> name = {}, length = {}".format(MODEL_NAME, args.size))

    # Training the Model
    train_start = time.time()
    iter_counter = 0
    for epoch in range(NUM_EPOCHS):
        epoch_start = time.time()
        for batch_index in range(NUM_BATCHES):
            print("------worker {} epoch {} batch {}------"
                  .format(args.rank, epoch, batch_index))
            batch_start = time.time()

            loss = 0.0

            # pull latest model
            ps_client.can_pull(stub, MODEL_NAME, iter_counter, args.rank)
            pull_start = time.time()
            latest_model = ps_client.pull_model(stub, MODEL_NAME, iter_counter, args.rank)
            pull_time = time.time() - pull_start

            # push gradient to PS
            w_b_grad = np.random.rand(1, args.size).astype(np.double).flatten()
            ps_client.can_push(stub, MODEL_NAME, iter_counter, args.rank)
            push_start = time.time()
            ps_client.push_grad(stub, MODEL_NAME, w_b_grad, LEARNING_RATE, iter_counter, args.rank)
            push_time = time.time() - push_start
            ps_client.can_pull(stub, MODEL_NAME, iter_counter + 1, args.rank)  # sync all workers

            print('Epoch: [%d/%d], Step: [%d/%d] >>> Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: pull model cost %.4f s, push update cost %.4f s'
                  % (epoch + 1, NUM_EPOCHS, batch_index, NUM_BATCHES,
                     time.time() - train_start, loss, time.time() - epoch_start,
                     time.time() - batch_start, pull_time, push_time))
            iter_counter += 1

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))


if __name__ == "__main__":
    main()
