import argparse

import os
import sys

import numpy as np
import torch
import torch.distributed as dist

from math import ceil
from torch.multiprocessing import Process

sys.path.append("../")
sys.path.append("../../")

from archived.ec2.admm_trainer_fl import ADMMTrainer
from archived.ec2 import partition_higgs
from archived.old_model import LogisticRegression


validation_ratio = .1
random_seed = 42


def dist_is_initialized():
    if dist.is_available():
        if dist.is_initialized():
            return True
    return False


def run(args):
    """ Distributed Synchronous SGD Example """
    device = torch.device('cuda' if torch.cuda.is_available() and not args.no_cuda else 'cpu')
    torch.manual_seed(random_seed)
    np.random.seed(random_seed)

    file_name = "{}/{}_{}".format(args.root, args.rank, args.world_size)
    print("read file {}".format(file_name))
    train_loader, test_loader = partition_higgs(args.batch_size, file_name, validation_ratio)
    num_batches = ceil(len(train_loader.dataset) / float(args.batch_size))

    model = LogisticRegression(args.features, args.classes).float()
    optimizer = torch.optim.SGD(model.parameters(), lr=args.learning_rate)
    criterion = torch.nn.CrossEntropyLoss()

    trainer = ADMMTrainer(model, optimizer, criterion, train_loader, test_loader,
                          args.lam, args.rho, device)

    trainer.fit(args.admm_epochs, args.epochs, is_dist=dist_is_initialized())


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
    parser.add_argument('--root', type=str, default='data')
    parser.add_argument('--no-cuda', action='store_true')
    parser.add_argument('--shuffle', type=bool, default=True)
    parser.add_argument('--admm-epochs', type=int, default=10)
    parser.add_argument('--epochs', type=int, default=10)
    parser.add_argument('-lr', '--learning-rate', type=float, default=1e-3)
    parser.add_argument('--batch-size', type=int, default=128)
    parser.add_argument('--classes', type=int, default=2)
    parser.add_argument('--features', type=int, default=30)
    parser.add_argument('--lam', type=float, default=0.01)
    parser.add_argument('--rho', type=float, default=0.01)
    args = parser.parse_args()
    print(args)

    if args.world_size > 1:
        dist.init_process_group(backend=args.backend, init_method=args.init_method, world_size=args.world_size, rank=args.rank)

    run(args)


if __name__ == '__main__':
    main()
