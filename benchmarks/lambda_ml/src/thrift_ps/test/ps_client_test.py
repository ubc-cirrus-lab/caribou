import time
import numpy as np
import argparse

import sys
sys.path.append("../../")
from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift_ps import constants

# algorithm setting
NUM_EPOCHS = 10
NUM_BATCHES = 10
MODEL_NAME = "w.b"
LEARNING_RATE = 0.1

# python ps_client_test.py --num-workers 1 --rank 0 --host spaceml1 --port 27000 --size 1000000


def main():
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('--num-workers', type=int, default=1)
    parser.add_argument('--rank', type=int, default=0)
    parser.add_argument('--host', type=str, default=constants.HOST)
    parser.add_argument('--port', type=int, default=constants.PORT)
    parser.add_argument('--size', type=int, default=100)
    args = parser.parse_args()
    print(args)

    print("host = {}".format(args.host))
    print("port = {}".format(args.port))

    # Set thrift connection
    # Make socket
    transport = TSocket.TSocket(args.host, args.port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    t_client = ParameterServer.Client(protocol)
    # Connect!
    transport.open()

    # test thrift connection
    ps_client.ping(t_client)
    print("create and ping thrift server >>> HOST = {}, PORT = {}".format(args.host, args.port))

    # register model
    ps_client.register_model(t_client, args.rank, MODEL_NAME, args.size, args.num_workers)
    ps_client.exist_model(t_client, MODEL_NAME)
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
            ps_client.can_pull(t_client, MODEL_NAME, iter_counter, args.rank)
            pull_start = time.time()
            latest_model = ps_client.pull_model(t_client, MODEL_NAME, iter_counter, args.rank)
            pull_time = time.time() - pull_start

            cal_start = time.time()
            w_b_grad = np.random.rand(1, args.size).astype(np.double).flatten()
            cal_time = time.time() - cal_start

            # push gradient to PS
            ps_client.can_push(t_client, MODEL_NAME, iter_counter, args.rank)
            push_start = time.time()
            ps_client.push_grad(t_client, MODEL_NAME, w_b_grad, LEARNING_RATE, iter_counter, args.rank)
            push_time = time.time() - push_start
            ps_client.can_pull(t_client, MODEL_NAME, iter_counter + 1, args.rank)  # sync all workers

            print('Epoch: [%d/%d], Step: [%d/%d] >>> Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: cal cost %.4f s, pull model cost %.4f s, push update cost %.4f s'
                  % (epoch + 1, NUM_EPOCHS, batch_index, NUM_BATCHES,
                     time.time() - train_start, loss, time.time() - epoch_start,
                     time.time() - batch_start, cal_time, pull_time, push_time))
            iter_counter += 1

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))


if __name__ == "__main__":
    main()
