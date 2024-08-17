from data_loader.libsvm_dataset import SparseDatasetWithLines, DenseDatasetWithLines
from thrift_ps.ps_service import ParameterServer
from thrift_ps.client import ps_client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from thrift_ps import constants

# algorithm setting
BATCH_SIZE = 10000
NUM_EPOCHS = 10
VALIDATION_RATIO = .2
SHUFFLE_DATASET = True
RANDOM_SEED = 42


def handler(event, context):
    start_time = time.time()
    bucket = event['bucket_name']
    key = urllib.parse.unquote_plus(event['key'], encoding='utf-8')
    key_splits = key.split("_")
    worker_index = int(key_splits[0])
    num_worker = int(key_splits[1])

    num_epochs = event['num_epochs']
    num_features = event['num_features']
    num_clusters = event['num_clusters']
    threshold = event["threshold"]
    dataset_type = event["dataset_type"]

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

    avg_error = np.iinfo(np.int16).max

    # Reading data from S3
    event_start = time.time()
    file = get_object(bucket, key).read().decode('utf-8').split("\n")
    s3_end = time.time()
    print(f"Getting object from s3 takes {s3_end - event_start}s")
    if dataset_type == "dense":
        # dataset is stored as numpy array
        dataset = DenseDatasetWithLines(file, num_features).ins_np
        dt = dataset.dtype
        centroid_shape = (num_clusters, dataset.shape[1])
    else:
        # dataset is sparse, stored as sparse tensor
        dataset = SparseDatasetWithLines(file, num_features)
        first_entry = dataset.ins_list[0].to_dense().numpy()
        dt = first_entry.dtype
        centroid_shape = (num_clusters, first_entry.shape[1])
    parse_end = time.time()
    print(f"Parsing dataset takes {parse_end - s3_end}s")
    print(
        f"Dataset: {dataset_type}, dtype: {dt}. Centroids shape: {centroid_shape}. num_features: {num_features}")

    # register model
    model_name = "kmeans"
    model_length = centroid_shape[0] * centroid_shape[1] + 1
    ps_client.register_model(t_client, worker_index, model_name, model_length, num_worker)
    ps_client.exist_model(t_client, model_name)
    print("register and check model >>> name = {}, length = {}".format(model_name, model_length))

    if worker_index == 0:
        if dataset_type == "dense":
            centroids = dataset[0:num_clusters].flatten()
        else:
            centroids = store_centroid_as_numpy(dataset.ins_list[0:num_clusters], num_clusters)
        ps_client.can_push(t_client, model_name, 0, worker_index)
        ps_client.push_grad(t_client, model_name, np.append(centroids.flatten(), 1), 0, 0, worker_index)
        ps_client.can_pull(t_client, model_name, 0, worker_index)  # sync all workers
    else:
        cent = ps_client.pull_model(t_client, model_name, 0, worker_index)
        centroid_size = len(cent) - 1
        centroids = np.array(cent[0:-1]).reshape(num_clusters, int(centroid_size / num_clusters))
        if centroid_shape != centroids.shape:
            logger.error("The shape of centroids does not match.")
        logger.info(f"Waiting for initial centroids takes {time.time() - parse_end} s")

    training_start = time.time()
    for epoch in range(1, num_epochs + 1):
        epoch_start = time.time()
        logger.info(f"{worker_index}-th worker in {epoch}-th epoch")

        last_epoch = epoch - 1
        ps_client.can_pull(t_client, model_name, last_epoch, worker_index)
        cent_with_error = ps_client.pull_model(t_client, model_name, last_epoch, worker_index)
        wait_end = time.time()

        print(f"Wait for centroid for {last_epoch}-th epoch. Takes {wait_end - epoch_start}")
        avg_error, centroids = cent_with_error[-1], np.array(cent_with_error[0:-1]).reshape(num_clusters, int(centroid_size/num_clusters))
        res = get_new_centroids(dataset, dataset_type, centroids, epoch, num_features, num_clusters, dt)
        print(f"{worker_index}-th worker: computation takes {time.time() - wait_end}s")

        dt = res.dtype
        sync_start = time.time()
        ps_client.can_push(t_client, model_name, epoch, worker_index)
        ps_client.push_grad(t_client, model_name, res, 0, epoch, worker_index)
        ps_client.can_pull(t_client, model_name, epoch + 1, worker_index)  # sync all workers
        print(
            f"{worker_index}-th worker finished the {epoch}-th epoch. Synchronization takes: {time.time() - sync_start}s.")

    logger.info(f"{worker_index}-th worker finished training. Error = {avg_error}, centroids = {centroids}")
    logger.info(f"Whole process time : {time.time() - training_start}")
    return

