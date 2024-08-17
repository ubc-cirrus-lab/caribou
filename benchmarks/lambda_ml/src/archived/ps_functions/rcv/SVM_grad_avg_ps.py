import time
import urllib.parse
import urllib.parse

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift_ps import constants
from thrift_ps.client import ps_client
from thrift_ps.ps_service import ParameterServer

# algorithm setting
NUM_FEATURES = 30
NUM_CLASSES = 2
LEARNING_RATE = 0.1
BATCH_SIZE = 10000
NUM_EPOCHS = 10
VALIDATION_RATIO = .2
SHUFFLE_DATASET = True
RANDOM_SEED = 42


def handler(event, context):
    startTs = time.time()
    num_features = event['num_features']
    learning_rate = event["learning_rate"]
    batch_size = event["batch_size"]
    num_epochs = event["num_epochs"]
    validation_ratio = event["validation_ratio"]

    # Reading data from S3
    bucket_name = event['bucket_name']
    key = urllib.parse.unquote_plus(event['key'], encoding='utf-8')
    print(f"Reading training data from bucket = {bucket_name}, key = {key}")
    key_splits = key.split("_")
    worker_index = int(key_splits[0])
    num_worker = int(key_splits[1])

    # read file from s3
    file = get_object(bucket_name, key).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - startTs))

    parse_start = time.time()
    dataset = SparseDatasetWithLines(file, num_features)
    print("parse data cost {} s".format(time.time() - parse_start))

    preprocess_start = time.time()
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    np.random.seed(42)
    np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    train_set = [dataset[i] for i in train_indices]
    val_set = [dataset[i] for i in val_indices]

    print("preprocess data cost {} s".format(time.time() - preprocess_start))

    # Set thrift connection
    # Make socket
    transport = TSocket.TSocket(constants.HOST, constants.PORT)
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
    print("create and ping thrift server >>> HOST = {}, PORT = {}"
          .format(constants.HOST, constants.PORT))

    svm = SparseSVM(train_set, val_set, num_features, num_epochs, learning_rate, batch_size)

    # register model
    model_name = "w.b"
    model_length = num_features
    ps_client.register_model(t_client, worker_index, model_name, model_length, num_worker)
    ps_client.exist_model(t_client, model_name)
    print("register and check model >>> name = {}, length = {}".format(model_name, model_length))

    # Training the Model
    train_start = time.time()
    iter_counter = 0

    # Training the Model
    for epoch in range(num_epochs):
        epoch_start = time.time()
        num_batches = math.floor(len(train_set) / batch_size)
        print(f"worker {worker_index} epoch {epoch}")

        for batch_idx in range(num_batches):
            batch_start = time.time()
            # pull latest model
            ps_client.can_pull(t_client, model_name, iter_counter, worker_index)
            latest_model = ps_client.pull_model(t_client, model_name, iter_counter, worker_index)
            svm.weights = torch.from_numpy(latest_model).reshape(num_features, 1)

            batch_ins, batch_label = svm.next_batch(batch_idx)
            acc = svm.one_epoch(batch_idx, epoch)
            compute_end = time.time()

            sync_start = time.time()
            w_update = svm.weights - latest_model
            ps_client.can_push(t_client, model_name, iter_counter, worker_index)
            ps_client.push_update(t_client, model_name, w_update, learning_rate, iter_counter, worker_index)
            ps_client.can_pull(t_client, model_name, iter_counter + 1, worker_index)  # sync all workers
            sync_time = time.time() - sync_start

            print('Epoch: [%d/%d], Step: [%d/%d] >>> Time: %.4f, train acc: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: cal cost %.4f s and communication cost %.4f s'
                  % (epoch + 1, NUM_EPOCHS, batch_idx + 1, len(train_indices)/batch_size,
                     time.time() - train_start, acc, time.time() - epoch_start,
                     time.time() - batch_start, compute_end-batch_start, sync_time))
            iter_counter += 1

        val_acc = svm.evaluate()
        print("Epoch takes {}s, validation accuracy: {}"
              .format(time.time() - epoch_start, val_acc))
