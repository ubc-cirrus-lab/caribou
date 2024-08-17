import time
import numpy as np

from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# algorithm setting
NUM_EPOCHS = 10
NUM_BATCHES = 1
MODEL_NAME = "w.b"
LEARNING_RATE = 0.01


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

    # Set thrift connection
    # Make socket
    transport = TSocket.TSocket(host, port)
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
    print("create and ping thrift server >>> HOST = {}, PORT = {}".format(host, port))

    # register model
    ps_client.register_model(t_client, worker_index, MODEL_NAME, size, num_workers)
    ps_client.exist_model(t_client, MODEL_NAME)
    print("register and check model >>> name = {}, length = {}".format(MODEL_NAME, size))

    # Training the Model
    train_start = time.time()
    iter_counter = 0
    for epoch in range(NUM_EPOCHS):
        epoch_start = time.time()
        for batch_index in range(NUM_BATCHES):
            print("------worker {} epoch {} batch {}------"
                  .format(worker_index, epoch, batch_index))
            batch_start = time.time()

            loss = 0.0

            # pull latest model
            ps_client.can_pull(t_client, MODEL_NAME, iter_counter, worker_index)
            pull_start = time.time()
            latest_model = ps_client.pull_model(t_client, MODEL_NAME, iter_counter, worker_index)
            pull_time = time.time() - pull_start

            w_b_grad = np.random.rand(1, size).astype(np.double).flatten()

            # push gradient to PS
            ps_client.can_push(t_client, MODEL_NAME, iter_counter, worker_index)
            push_start = time.time()
            ps_client.push_grad(t_client, MODEL_NAME, w_b_grad, LEARNING_RATE, iter_counter, worker_index)
            push_time = time.time() - push_start
            ps_client.can_pull(t_client, MODEL_NAME, iter_counter + 1, worker_index)  # sync all workers

            print('Epoch: [%d/%d], Step: [%d/%d] >>> Time: %.4f, Loss: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: pull model cost %.4f s, push update cost %.4f s'
                  % (epoch + 1, NUM_EPOCHS, batch_index, NUM_BATCHES,
                     time.time() - train_start, loss, time.time() - epoch_start,
                     time.time() - batch_start, pull_time, push_time))
            iter_counter += 1

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
