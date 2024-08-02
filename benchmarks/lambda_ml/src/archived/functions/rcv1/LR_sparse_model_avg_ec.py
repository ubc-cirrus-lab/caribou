import time

from archived.s3.get_object import get_object
from archived.elasticache.Memcached import memcached_init
from archived.sync import reduce_epoch, clear_bucket

# lambda setting
local_dir = "/tmp"

# algorithm setting
shuffle_dataset = True
random_seed = 42


def handler(event, context):
    try:
        start_time = time.time()
        bucket_name = event['bucket_name']
        worker_index = event['rank']
        num_workers = event['num_workers']
        key = event['file']
        merged_bucket = event['merged_bucket']
        num_features = event['num_features']
        learning_rate = event["learning_rate"]
        batch_size = event["batch_size"]
        num_epochs = event["num_epochs"]
        validation_ratio = event["validation_ratio"]
        elasti_location = event['elasticache']
        endpoint = memcached_init(elasti_location)

        # read file from s3
        file = get_object(bucket_name, key).read().decode('utf-8').split("\n")
        print("read data cost {} s".format(time.time() - start_time))

        parse_start = time.time()
        dataset = SparseDatasetWithLines(file, num_features)
        print("parse data cost {} s".format(time.time() - parse_start))

        preprocess_start = time.time()
        dataset_size = len(dataset)
        indices = list(range(dataset_size))
        split = int(np.floor(validation_ratio * dataset_size))
        if shuffle_dataset:
            np.random.seed(random_seed)
            np.random.shuffle(indices)
        train_indices, val_indices = indices[split:], indices[:split]

        train_set = [dataset[i] for i in train_indices]
        val_set = [dataset[i] for i in val_indices]

        print("preprocess data cost {} s".format(time.time() - preprocess_start))
        lr = LogisticRegression(train_set, val_set, num_features, num_epochs, learning_rate, batch_size)

        # Training the Model
        train_start = time.time()
        for epoch in range(num_epochs):
            epoch_start = time.time()
            num_batches = math.floor(len(train_set) / batch_size)
            print(f"worker {worker_index} epoch {epoch}")
            for batch_idx in range(num_batches):
                batch_start = time.time()
                batch_ins, batch_label = lr.next_batch(batch_idx)
                batch_grad = torch.zeros(lr.n_input, 1, requires_grad=False)
                batch_bias = np.float(0)
                train_loss = Loss()
                train_acc = Accuracy()

                for i in range(len(batch_ins)):
                    z = lr.forward(batch_ins[i])
                    h = lr.sigmoid(z)
                    loss = lr.loss(h, batch_label[i])
                    #print("z= {}, h= {}, loss = {}".format(z, h, loss))
                    train_loss.update(loss, 1)
                    train_acc.update(h, batch_label[i])
                    g = lr.backward(batch_ins[i], h.item(), batch_label[i])
                    batch_grad.add_(g)
                    batch_bias += np.sum(h.item()-batch_label[i])
                batch_grad = batch_grad.div(len(batch_ins))
                batch_bias = batch_bias / len(batch_ins)
                batch_grad.mul_(-1.0 * learning_rate)
                lr.grad.add_(batch_grad)
                lr.bias = lr.bias - batch_bias * learning_rate
            cal_time = time.time() - epoch_start

            sync_start = time.time()
            np_grad = lr.grad.numpy().flatten()
            np_bias = np.array(lr.bias, dtype=np_grad.dtype)
            w_and_b = np.concatenate((np_grad, np_bias))
            postfix = "{}".format(epoch)
            w_b_merge = reduce_epoch(endpoint, w_and_b, merged_bucket, num_workers, worker_index, postfix)
            lr.grad, lr.bias = w_b_merge[:-1].reshape(num_features, 1) / float(num_workers), \
                               float(w_b_merge[-1]) / float(num_workers)
            sync_time = time.time() - sync_start

            test_start = time.time()
            val_loss, val_acc = lr.evaluate()
            test_time = time.time() - test_start

            print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f, Loss: %s, Accuracy: %s, epoch cost %.4f, '
                  'cal cost %.4f s, sync cost %.4f s, test cost %.4f s, '
                  'test accuracy: %s %%, test loss: %s'
                  % (epoch + 1, num_epochs, batch_idx + 1, num_batches,
                     time.time() - train_start, train_loss, train_acc, time.time() - epoch_start,
                     cal_time, sync_time, test_time, val_acc, val_loss))

        if worker_index == 0:
            clear_bucket(endpoint)
        print("elapsed time = {} s".format(time.time() - start_time))

    except Exception as e:
        print("Error {}".format(e))
