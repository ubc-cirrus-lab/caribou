import argparse
import os
import sys
import time

from torch.multiprocessing import Process

sys.path.append("../../")

from archived.ec2 import partition_sparse


def dist_is_initialized():
    if dist.is_available():
        if dist.is_initialized():
            return True
    return False


def broadcast_average(args, weights):
    dist.all_reduce(weights, op=dist.ReduceOp.SUM)
    return weights.float() * 1/args.world_size


def run(args):
    device = torch.device('cuda' if torch.cuda.is_available() and not args.no_cuda else 'cpu')
    read_start = time.time()
    torch.manual_seed(1234)

    train_file = open(args.train_file, 'r').readlines()
    dataset = partition_sparse(train_file, args.features)
    print(f"Loading dataset costs {time.time() - read_start}s")

    preprocess_start = time.time()
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(0.2 * dataset_size))
    if args.shuffle:
        np.random.seed(42)
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    train_set = [dataset[i] for i in train_indices]
    val_set = [dataset[i] for i in val_indices]
    print("preprocess data cost {} s".format(time.time() - preprocess_start))

    svm = SparseSVM(train_set, val_set, args.features, args.epochs, args.learning_rate, args.batch_size)
    training_start = time.time()
    for epoch in range(args.epochs):
        num_batches = math.floor(len(train_set)/args.batch_size)
        start_compute = time.time()
        for batch_idx in range(num_batches):
            batch_start = time.time()
            batch_acc = svm.one_epoch(batch_idx, epoch)
            print(f"{args.rank}-th worker . Takes {time.time() - batch_start}")
            print(f"Batch accuracy: {batch_acc}")
            batch_end = time.time()
            svm.weights = broadcast_average(args, svm.weights)
            print(f"{args.rank}-th worker finishes sychronizing. Takes {time.time() - batch_end}")

        val_acc = svm.evaluate()
        print(f"validation accuracy: {val_acc}")
        print(f"Epoch takes {time.time() - start_compute}s")

    print(f"Finishes training. {args.epochs} takes {time.time() - training_start}s.")


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
    parser.add_argument('-l', '--learning-rate', type=float, default=1e-3)
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
