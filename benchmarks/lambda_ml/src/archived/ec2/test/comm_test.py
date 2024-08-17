import argparse

import os
import sys
import time

import torch
import torch.distributed as dist

from torch.multiprocessing import Process

sys.path.append("../")
sys.path.append("../../")

SLEEP_INTERVAL = 10


def dist_is_initialized():
    if dist.is_available():
        if dist.is_initialized():
            return True
    return False


def reduce_data(t):
    size = float(dist.get_world_size())
    dist.all_reduce(t, op=dist.ReduceOp.SUM)
    t /= size


def run(args):
    """ Distributed Synchronous SGD Example """
    device = torch.device('cuda' if torch.cuda.is_available() and not args.no_cuda else 'cpu')
    torch.manual_seed(1234)

    for epoch in range(args.epochs):
        epoch_start = time.time()
        time.sleep(SLEEP_INTERVAL)
        t = torch.rand(1, args.data_size)
        sync_start = time.time()
        if dist_is_initialized():
            reduce_data(t)
        print("Epoch {} cost {} s, sync cost {} s".format(epoch, time.time()-epoch_start, time.time()-sync_start))


def init_processes(rank, size, fn, backend='gloo'):
    """ Initialize the distributed environment. """
    os.environ['MASTER_ADDR'] = '127.0.0.1'
    os.environ['MASTER_PORT'] = '29500'
    dist.init_process_group(backend, rank=rank, world_size=size)
    fn(rank, size)


def run_local():
    size = 2
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
    parser.add_argument('--data-size', type=int, default=100)
    args = parser.parse_args()
    print(args)

    if args.world_size > 1:
        dist.init_process_group(backend=args.backend, init_method=args.init_method, world_size=args.world_size, rank=args.rank)

    run(args)


if __name__ == '__main__':
    main()
