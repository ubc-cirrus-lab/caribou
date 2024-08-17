import time
import urllib.parse

# lambda setting
grad_bucket = "sparse-grads"
model_bucket = "sparse-updates"
local_dir = "/tmp"
w_prefix = "w_"
b_prefix = "b_"
w_grad_prefix = "w_grad_"
b_grad_prefix = "b_grad_"

shuffle_dataset = True
random_seed = 42


def handler(event, context):
    try:
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
        if shuffle_dataset:
            np.random.seed(random_seed)
            np.random.shuffle(indices)
        train_indices, val_indices = indices[split:], indices[:split]

        train_set = [dataset[i] for i in train_indices]
        val_set = [dataset[i] for i in val_indices]

        print("preprocess data cost {} s".format(time.time() - preprocess_start))
        lr = LogisticRegression(train_set, val_set, num_features, num_epochs, learning_rate, batch_size)

        # Training the Model
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

                np_grad = lr.grad.numpy().flatten()
                np_bias = np.array(lr.bias, dtype=np_grad.dtype)
                print(f"computation takes {time.time() - batch_start}s, bias: {lr.bias}")
                print(f"np_grad type: {np_grad.dtype}, np_bias type: {np_bias.dtype}")

                sync_start = time.time()
                put_object(grad_bucket, w_grad_prefix + str(worker_index), np_grad.tobytes())
                put_object(grad_bucket, b_grad_prefix + str(worker_index), np_bias.tobytes())
                file_postfix = "{}_{}".format(epoch, batch_idx)
                if worker_index == 0:
                    w_grad_merge, b_grad_merge = \
                        merge_w_b_grads(grad_bucket, num_worker, np_grad.dtype, np_grad.shape, np_bias.shape,
                                        w_grad_prefix, b_grad_prefix)
                    b_grad_merge = np.array(b_grad_merge, dtype=w_grad_merge.dtype)
                    put_merged_w_b_grad(model_bucket, w_grad_merge, b_grad_merge,
                                        file_postfix, w_grad_prefix, b_grad_prefix)
                    delete_expired_w_b(model_bucket, epoch, batch_idx, w_grad_prefix, b_grad_prefix)
                    lr.grad = torch.from_numpy(w_grad_merge).reshape(num_features, 1)
                    lr.bias = float(b_grad_merge)
                    print(f"bias {lr.bias}")
                else:
                    w_grad_merge, b_grad_merge = get_merged_w_b_grad(model_bucket, file_postfix,
                                                                     np_grad.dtype, np_grad.shape,
                                                                     np_bias.shape, w_grad_prefix, b_grad_prefix)
                    lr.grad = torch.from_numpy(w_grad_merge).reshape(num_features, 1)
                    lr.bias = float(b_grad_merge)
                    print(f"bias: {lr.bias}")
                print(f"synchronization cost {time.time() - sync_start}s")
                print(f"batch takes {time.time() - batch_start}s")

                if (batch_idx + 1) % 10 == 0:
                    print(f"Epoch: {epoch + 1}/{num_epochs}, Step: {batch_idx + 1}/{len(train_indices) / batch_size}, "
                          f"Loss: {train_loss}")

            val_loss, val_acc = lr.evaluate()
            print(f"Validation loss: {val_loss}, validation accuracy: {val_acc}")
            print(f"Epoch takes {time.time() - epoch_start}s")

        if worker_index == 0:
            clear_bucket(model_bucket)
            clear_bucket(grad_bucket)
        print("elapsed time = {} s".format(time.time() - startTs))

    except Exception as e:
        print("Error {}".format(e))