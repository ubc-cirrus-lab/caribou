import argparse
import time
import os
import sys
import torch
import torch.distributed as dist
import numpy as np
import logging
sys.path.append("../../")

from archived.ec2 import partition_agaricus
from archived.pytorch_model import SparseKmeans


def broadcast_average(args, centroid_tensor, error_tensor):
    if args.communication == "all-reduce":
        dist.all_reduce(centroid_tensor, op=dist.ReduceOp.SUM)
        dist.all_reduce(error_tensor, op=dist.ReduceOp.SUM)
        return centroid_tensor/args.world_size, error_tensor/args.world_size
    if args.communication == "centralized":
        all_centroids_list = []
        avg_cent = torch.empty(args.num_clusters, args.features)
        if args.rank == 0:
            torch.distributed.gather(centroid_tensor, all_centroids_list)
            tensor_sum = torch.empty(args.num_clusters, args.features)
            for c in all_centroids_list:
                tensor_sum += c
            avg_cent = c/args.world_size
            dist.broadcast(avg_cent, 0)
        else:
            dist.send(centroid_tensor, 0)
        return avg_cent


def run(args):
    device = torch.device('cpu')
    torch.manual_seed(1234)
    read_start = time.time()
    avg_error = np.iinfo(np.int16).max
    logging.info(f"Worker {args.rank} starts.")

    train_file = open("/home/ubuntu/LambdaML/dataset/agaricus_127d_train.libsvm", 'r').readlines()
    test_file = open("/home/ubuntu/LambdaML/dataset/agaricus_127d_test.libsvm", 'r').readlines()
    logging.info(f"Reading train and test files.")

    train_set, _, _, test_set = partition_agaricus(1, train_file, test_file)
    train_set = [t[0] for t in train_set]
    logging.info(f"Loading dataset costs {time.time() - read_start}s")

    # initialize centroids
    init_cent_start = time.time()
    if args.rank == 0:
        c_dense_list = [t.to_dense() for t in train_set[:args.num_clusters]]
        centroids = torch.stack(c_dense_list).reshape(args.num_clusters, args.features)
    else:
        centroids = torch.empty(args.num_clusters, args.features)
    dist.broadcast(centroids, 0)
    logging.info(f"Receiving initial centroids costs {time.time() - init_cent_start}s")

    training_start = time.time()
    for epoch in range(args.epochs):
        if avg_error >= args.threshold:
            start_compute = time.time()
            model = SparseKmeans(train_set, centroids, args.features, args.num_clusters)
            model.find_nearest_cluster()
            error = torch.tensor(model.error)
            end_compute = time.time()
            logging.info(f"{args.rank}-th worker computing centroids takes {end_compute - start_compute}s")
            centroids, avg_error = broadcast_average(args, model.get_centroids("dense_tensor"), error)
            logging.info(f"{args.rank}-th worker finished communicating the result. Takes {time.time() - end_compute}s")
        else:
            logging.info(f"{args.rank}-th worker finished training. Error = {avg_error}, centroids = {centroids}")
            logging.info(f"Whole process time : {time.time() - training_start}")
            return


def init_processes(rank, size, fn, backend='gloo'):
    """ Initialize the distributed environment. """
    os.environ['MASTER_ADDR'] = '127.0.0.1'
    os.environ['MASTER_PORT'] = '29500'
    dist.init_process_group(backend, rank=rank, world_size=size)
    fn(rank, size)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', type=str, default='gloo', help='Name of the backend to use.')
    parser.add_argument(
        '-i',
        '--init-method',
        type=str,
        default='tcp://127.0.0.1:23456',
        help='URL specifying how to initialize the package.')
    parser.add_argument('-s', '--world-size', type=int, default=16, help='Number of processes participating in the job.')
    parser.add_argument('-r', '--rank', type=int, default=0, help='Rank of the current process.')
    parser.add_argument('--epochs', type=int, default=20)
    parser.add_argument('--no-cuda', action='store_true')
    parser.add_argument('-k', '--num-clusters', type=int, default=20)
    parser.add_argument('--batch-size', type=int, default=128)
    parser.add_argument('--threshold', type=float, default=0.02)
    parser.add_argument('--features', type=int, default=127)
    parser.add_argument('--communication', type=str, default='all-reduce')
    args = parser.parse_args()
    logging.basicConfig(filename=f"/home/ubuntu/log/agaricus_r{args.rank}_w{args.world_size}_k{args.num_clusters}", filemode='a', level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')

    logging.info(args)

    dist.init_process_group(
            backend=args.backend,
            init_method=args.init_method,
            world_size=args.world_size,
            rank=args.rank,
        )
    run(args)


if __name__ == '__main__':
    main()
