import argparse
import os
import sys
import time

from torch.multiprocessing import Process

sys.path.append("../../")

validation_ratio = .1


def dist_is_initialized():
    if dist.is_available():
        if dist.is_initialized():
            return True
    return False


def broadcast_average(world_size, weights):
    dist.all_reduce(weights, op=dist.ReduceOp.SUM)
    #return weights.float() * 1 / world_size
    return weights.float()


def run(args):
    device = torch.device('cuda' if torch.cuda.is_available() and not args.no_cuda else 'cpu')
    read_start = time.time()
    torch.manual_seed(1234)

    file_name = "{}/{}_{}".format(args.root, args.rank, args.world_size)
    file = open(file_name, 'r').readlines()
    #dataset = partition_sparse(file, args.features)
    dataset = SparseDatasetWithLines(file, args.features)
    print("Loading dataset costs {}s".format(time.time() - read_start))

    preprocess_start = time.time()
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    if args.shuffle:
        np.random.seed(42)
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    train_set = [dataset[i] for i in train_indices]
    val_set = [dataset[i] for i in val_indices]
    print("preprocess data cost {} s".format(time.time() - preprocess_start))

    svm = SparseSVM2(train_set, val_set, args.features, args.epochs, args.learning_rate, args.batch_size)
    train_start = time.time()
    for epoch in range(args.epochs):
        epoch_start = time.time()
        num_batches = math.floor(len(train_set) / args.batch_size)
        cal_time = 0
        sync_time = 0
        epoch_loss = 0.0
        epoch_acc = 0.0
        for batch_idx in range(num_batches):
            batch_start = time.time()
            batch_loss, batch_acc = svm.one_batch(batch_idx, epoch)
            epoch_loss += batch_loss.average
            epoch_acc += batch_acc.accuracy

            # batch_grad = torch.zeros(svm.n_input, 1, requires_grad=False)
            #             # batch_ins, batch_label = svm.next_batch(batch_idx)
            #             # for i in range(len(batch_ins)):
            #             #     h = svm.forward(batch_ins[i])
            #             #     loss = svm.loss(h, batch_label[i])
            #             #     epoch_loss.update(loss, 1)
            #             #     epoch_acc.update(h, batch_label[i])
            #             #     g = svm.backward(loss.item(), batch_ins[i], batch_label[i])
            #             #     batch_grad.add_(g)
            #             # batch_grad = batch_grad.div(len(batch_ins))
            #             # batch_grad.mul_(-1.0 * args.learning_rate)
            #             # svm.weights.add_(batch_grad)

            cal_time += time.time() - batch_start

            sync_start = time.time()
            if dist_is_initialized():
                weights = np.append(svm.weights.numpy().flatten())
                weights_merged = broadcast_average(args.world_size, torch.tensor(weights))
                svm.weights = weights_merged.reshape(args.features, 1)
            sync_time += time.time() - sync_start

        test_start = time.time()
        epoch_loss /= num_batches
        epoch_acc /= num_batches
        val_loss, val_acc = svm.evaluate()
        test_time = time.time() - test_start

        print('Epoch: [%d/%d], Step: [%d/%d], Time: %.4f, Loss: %s, Accuracy: %s, epoch cost %.4f, '
              'cal cost %.4f s, sync cost %.4f s, test cost %.4f s, '
              'test accuracy: %s %%, test loss: %s'
              % (epoch + 1, args.epochs, batch_idx + 1, num_batches,
                 time.time() - train_start, epoch_loss, epoch_acc, time.time() - epoch_start,
                 cal_time, sync_time, test_time, val_acc, val_loss))

    print("Finishes training. {} epochs takes {}s.".format(args.epochs, time.time() - train_start))


def init_processes(rank, size, fn, backend='gloo'):
    """ Initialize the distributed environment. """
    os.environ['MASTER_ADDR'] = '127.0.0.1'
    os.environ['MASTER_PORT'] = '29500'
    dist.init_process_group(backend, rank=rank, world_size=size)
    fn(rank, size)


def run_local(size):
    processes = []
    for rank in range(size):
        p = Process(target=init_processes, args=(rank, size, run))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', type=str, default='gloo', help='Name of the backend to use.')
    parser.add_argument(
        '-i',
        '--init-method',
        type=str,
        default='tcp://127.0.0.1:23456',
        help='URL specifying how to initialize the package.')
    parser.add_argument('-s', '--world-size', type=int, default=1, help='Number of processes participating in the job.')
    parser.add_argument('-r', '--rank', type=int, default=0, help='Rank of the current process.')
    parser.add_argument('--epochs', type=int, default=20)
    parser.add_argument('--no-cuda', action='store_true')
    parser.add_argument('-lr', '--learning-rate', type=float, default=1e-3)
    parser.add_argument('--root', type=str, default='data')
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--features', type=int, default=47236)
    parser.add_argument('--shuffle', type=int, default=1)
    parser.add_argument('--train-file', type=str, default='data')
    args = parser.parse_args()
    print(args)

    if args.world_size > 1:
        dist.init_process_group(backend=args.backend, init_method=args.init_method, world_size=args.world_size, rank=args.rank)

    run(args)
    # run_local(args.world_size)


if __name__ == '__main__':
    main()
