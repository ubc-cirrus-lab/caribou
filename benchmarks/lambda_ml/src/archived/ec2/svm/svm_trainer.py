import time

import torch
import torch.nn.functional as F
from torch import distributed as dist
import torch.nn
#from utils.print_utils import progress_bar


class Average(object):

    def __init__(self):
        self.sum = 0
        self.count = 0

    def __str__(self):
        return '{:.6f}'.format(self.average)

    @property
    def average(self):
        return self.sum / self.count

    def update(self, value, number):
        self.sum += value * number
        self.count += number


class Accuracy(object):

    def __init__(self):
        self.correct = 0
        self.count = 0

    def __str__(self):
        return '{:.2f}%'.format(self.accuracy * 100)

    @property
    def accuracy(self):
        return self.correct / self.count

    def update(self, output, target):
        with torch.no_grad():
            pred = output.argmax(dim=1)
            correct = pred.eq(target).sum().item()

        self.correct += correct
        self.count += output.size(0)


class Trainer(object):

    def __init__(self, model, optimizer, train_loader, test_loader, device=torch.device("cpu")):
        self.model = model
        self.optimizer = optimizer
        self.train_loader = train_loader
        self.test_loader = test_loader
        self.device = device
        self.train_start = time.time()

    def average_gradients(self):
        """ Gradient averaging. """
        size = float(dist.get_world_size())
        for param in self.model.parameters():
            dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
            param.grad.data /= size

    def fit(self, epochs, is_dist=True):
        for epoch in range(1, epochs + 1):
            train_loss, train_acc = self.train(epoch, is_dist)
            test_loss, test_acc = self.evaluate()
            print(
                'Epoch: {}/{},'.format(epoch, epochs),
                'train loss: {}, train acc: {},'.format(train_loss, train_acc),
                'test loss: {}, test acc: {}.'.format(test_loss, test_acc),
            )

    def train(self, epoch, is_dist=True):
        self.model.train()

        epoch_start = time.time()
        num_batch = 0
        total_sync_time = 0

        train_loss = Average()
        train_acc = Accuracy()

        for batch_idx, (data, target) in enumerate(self.train_loader):
            batch_start = time.time()

            data = data.to(self.device)
            target = target.to(self.device)

            output = self.model(data)
            loss = torch.mean(torch.clamp(1 - output.t() * target, min=0))  # hinge loss
            loss += 0.01 * torch.mean(self.model.linear.weight ** 2) / 2.0  # l2 penalty

            self.optimizer.zero_grad()
            loss.backward()

            sync_start = time.time()

            if is_dist:
                self.average_gradients()
            self.optimizer.step()

            sync_time = time.time() - sync_start
            total_sync_time += sync_time
            num_batch += 1

            train_loss.update(loss.item(), data.size(0))
            train_acc.update(output, target)

            batch_time = time.time() - batch_start

            # print('Epoch: %d, Batch: %d, Time: %.4f s, Loss: %.4f, '
            #       'batch cost %.4f s, communicator cost %.4f s, comp cost %.4f s'
            #       % (epoch + 1, batch_idx, time.time() - self.train_start,
            #          loss.data, batch_time, sync_time, batch_time - sync_time))

            #progress_bar(batch_idx, len(self.train_loader), 'Loss: {} | Acc: {}'.format(train_loss, train_acc))

        epoch_time = time.time() - epoch_start
        print("Epoch {} has {} batches, time = {} s, epoch cost {} s, sync time = {} s, cal time = {} s"
              .format(epoch, num_batch, time.time() - self.train_start,
                      epoch_time, total_sync_time, epoch_time - total_sync_time))

        return train_loss, train_acc

    def evaluate(self):
        self.model.eval()

        test_loss = Average()
        test_acc = Accuracy()

        with torch.no_grad():
            for data, target in self.test_loader:
                data = data.to(self.device)
                target = target.to(self.device)

                output = self.model(data)
                loss = F.cross_entropy(output, target)

                test_loss.update(loss.item(), data.size(0))
                test_acc.update(output, target)

        return test_loss, test_acc
