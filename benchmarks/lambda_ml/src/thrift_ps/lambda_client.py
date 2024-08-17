import sys
sys.path.append("../../")
import time

from thrift_ps.ps_service import ParameterServer
from thrift_ps.ps_service.ttypes import Model, Update, Grad, InvalidOperation

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from thrift_ps import constants

MODEL_ID = "weight"
MODEL_LENGTH = 10


def handler(event, context):
    start_time = time.time()
    worker_index = event['rank']
    num_workers = event['num_workers']

    # Make socket
    transport = TSocket.TSocket(constants.HOST, constants.PORT)
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
        client.register_model(MODEL_ID, MODEL_LENGTH, num_workers)

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
