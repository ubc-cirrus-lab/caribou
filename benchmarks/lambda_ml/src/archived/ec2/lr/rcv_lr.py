import argparse
import os
import sys
import time
import logging
from torch.multiprocessing import Process

sys.path.append("../../")

from data_loader.libsvm_dataset import SparseDatasetWithLines


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
    logging.info(f"{args.rank}-th worker starts.")

    read_start = time.time()
    torch.manual_seed(1234)
    train_file = open(args.train_file, 'r').readlines()
    dataset = SparseDatasetWithLines(train_file, args.features)
    logging.info(f"Loading dataset costs {time.time() - read_start}s")

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
    logging.info("preprocess data cost {} s".format(time.time() - preprocess_start))

    lr = LogisticRegression(train_set, val_set, args.features, args.epochs, args.learning_rate, args.batch_size)
    training_start = time.time()
    for epoch in range(args.epochs):
        epoch_start = time.time()
        num_batches = math.floor(len(train_set) / args.batch_size)
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
                # print("z= {}, h= {}, loss = {}".format(z, h, loss))
                train_loss.update(loss, 1)
                train_acc.update(h, batch_label[i])
                g = lr.backward(batch_ins[i], h.item(), batch_label[i])
                batch_grad.add_(g)
                batch_bias += np.sum(h.item() - batch_label[i])
            batch_grad = batch_grad.div(len(batch_ins))
            batch_bias = batch_bias / len(batch_ins)
            batch_grad.mul_(-1.0 * args.learning_rate)
            lr.grad.add_(batch_grad)
            lr.bias = lr.bias - batch_bias * args.learning_rate
            end_compute = time.time()
            logging.info(f"Train loss: {train_loss}, train accurary: {train_acc}")
            logging.info(f"{args.rank}-th worker finishes computing one batch. Takes {time.time() - batch_start}")

            weights = np.append(lr.grad.numpy().flatten(), lr.bias)
            weights_merged = broadcast_average(args, torch.tensor(weights))
            lr.grad, lr.bias = weights_merged[:-1].reshape(args.features, 1), float(weights_merged[-1])
            logging.info(f"{args.rank}-th worker finishes sychronizing. Takes {time.time() - end_compute}")

        val_loss, val_acc = lr.evaluate()
        logging.info(f"Validation loss: {val_loss}, validation accuracy: {val_acc}")
        logging.info(f"Epoch takes {time.time() - epoch_start}s")

    logging.info(f"Finishes training. {args.epochs} takes {time.time() - training_start}s.")



def init_processes(rank, size, fn, backend='gloo'):
    """ Initialize the distributed environment. """
    os.environ['MASTER_ADDR'] = '127.0.0.1'
    os.environ['MASTER_PORT'] = '23456'
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
        default='tcp://127.0.0.1:22222',
        help='URL specifying how to initialize the package.')
    parser.add_argument('-s', '--world-size', type=int, default=1, help='Number of processes participating in the job.')
    parser.add_argument('-r', '--rank', type=int, default=0, help='Rank of the current process.')
    parser.add_argument('--epochs', type=int, default=5)
    parser.add_argument('--no-cuda', action='store_true')
    parser.add_argument('-l', '--learning-rate', type=float, default=1e-3)
    parser.add_argument('--root', type=str, default='data')
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--features', type=int, default=47236)
    parser.add_argument('--train-file', type=str, default='data')
    parser.add_argument('--shuffle', type=int, default=1)
    args = parser.parse_args()
    logging.basicConfig(filename=f"/home/ubuntu/log/lr_rcv_r{args.rank}_w{args.world_size}_b{args.batch_size}", filemode='a', level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')
    logging.info(args)

    dist.init_process_group(backend=args.backend, init_method=args.init_method, world_size=args.world_size, rank=args.rank)
    run(args)
    # run_local(args.world_size)


if __name__ == '__main__':
    main()
