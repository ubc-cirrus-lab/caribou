import time

from archived.sync import reduce_scatter_batch_multi_bucket, delete_expired_merged

# algorithm setting
num_features = 10000000
num_epochs = 10
num_iters = 5
random_seed = 42


def handler(event, context):
    start_time = time.time()
    worker_index = event['rank']
    num_workers = event['num_workers']
    num_buckets = event['num_buckets']
    tmp_bucket_prefix = event['tmp_bucket_prefix']
    merged_bucket_prefix = event['merged_bucket_prefix']

    print('number of workers = {}'.format(num_workers))
    print('number of buckets = {}'.format(num_buckets))
    print('worker index = {}'.format(worker_index))

    # Training the Model
    train_start = time.time()
    for epoch in range(num_epochs):
        epoch_start = time.time()
        for iter in range(num_iters):
            print("------worker {} epoch {} batch {}------".format(worker_index, epoch, iter))
            batch_start = time.time()

            w = np.random.rand(1, num_features)
            w_shape = w.shape

            cal_time = time.time() - batch_start

            sync_start = time.time()
            postfix = "{}_{}".format(epoch, iter)
            w_merge = \
                reduce_scatter_batch_multi_bucket(w.flatten(), tmp_bucket_prefix, merged_bucket_prefix,
                                                  num_buckets, num_workers, worker_index, postfix)
            w_merge = w_merge.reshape(w_shape) / float(num_workers)

            sync_time = time.time() - sync_start

            print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f, epoch cost %.4f, '
                  'batch cost %.4f s: cal cost %.4f s and communication cost %.4f s'
                  % (epoch + 1, num_epochs, iter + 1, num_iters,
                     time.time() - train_start, time.time() - epoch_start,
                     time.time() - batch_start, cal_time, sync_time))

        if worker_index == 0:
            for i in range(num_buckets):
                delete_expired_merged("{}_{}".format(merged_bucket_prefix, i), epoch)

    if worker_index == 0:
        for i in range(num_buckets):
            clear_bucket("{}_{}".format(merged_bucket_prefix, i))
            clear_bucket("{}_{}".format(tmp_bucket_prefix, i))

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))
