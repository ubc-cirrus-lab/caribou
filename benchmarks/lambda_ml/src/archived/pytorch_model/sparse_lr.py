import math

import numpy as np
import torch
import torch.distributed as dist

from data_loader.libsvm_dataset import SparseDatasetWithLines


def dist_is_initialized():
    if dist.is_available():
        if dist.is_initialized():
            return True
    return False


class Loss(object):

    def __init__(self):
        self.sum = 0
        self.count = 0

    def __str__(self):
        return '{:.6f}'.format(self.average)

    @property
    def average(self):
        return self.sum / self.count

    def update(self, value, number):
        self.sum += value.item() * number
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
        pred = 1 if output.item() >= 0.5 else 0

        self.correct += 1 if pred == target else 1
        self.count += 1


class LogisticRegression(object):

    def __init__(self, _train_set, _test_set, _n_input, _epochs, _lr, _batch_size):
        self.train_set = _train_set
        self.test_set = _test_set
        self.num_train = self.train_set.__len__()
        self.num_test = self.test_set.__len__()
        self.n_input = _n_input
        self.grad = torch.zeros(self.n_input, 1, requires_grad=False)
        self.bias = 0
        self.num_epochs = _epochs
        self.lr = _lr
        self.batch_size = _batch_size
        self.cur_index = 0

    def next_batch(self, batch_idx):
        start = batch_idx * self.batch_size
        end = min((batch_idx + 1) * self.batch_size, self.num_train)
        ins = [ts[0] for ts in self.train_set[start:end]]
        label = [1 if ts[1] > 0 else 0 for ts in self.train_set[start:end]]
        return ins, label

    def get_batch(self):
        ins_list = []
        label_list = []
        num = 0
        while num < self.batch_size:
            ins, label = self.train_set.__getitem__(self.cur_index)
            ins_list.append(ins)
            label_list.append(label)
            num += 1
            self.cur_index += 1
            if self.cur_index >= self.num_train:
                self.cur_index = 0
        return ins_list, label_list

    def forward(self, ins):
        pred = torch.sparse.mm(ins, self.grad)
        pred = pred.add(self.bias)
        return pred

    def batch_forward(self, ins):
        pred = [torch.sparse.mm(ins, self.grad) for i in ins]
        return torch.tensor(pred)

    def sigmoid(self, z):
        return 1 / (1 + np.exp(-z))

    def batch_sigmoid(self, z):
        return 1 / (1 + np.exp(-z))

    def loss(self, h, y):
        return -y * np.log(h) - (1 - y) * np.log(1 - h)

    def backward(self, x, h, y):
        return x.transpose(0, 1) * (h - y)

    def one_epoch(self, is_dist=dist_is_initialized()):
        batch_ins, batch_label = self.get_batch()
        batch_grad = torch.zeros(self.n_input, 1, requires_grad=False)
        batch_bias = 0

        train_loss = Loss()
        train_acc = Accuracy()

        for i in range(len(batch_ins)):
            z = self.forward(batch_ins[i])
            h = self.sigmoid(z)
            loss = self.loss(h, batch_label[i])
            # print("z= {}, h= {}, loss = {}".format(z, h, loss))
            train_loss.update(loss, 1)
            train_acc.update(h, batch_label[i])
            g = self.backward(batch_ins[i], h.item(), batch_label[i])
            batch_grad.add_(g)
            batch_bias += np.sum(h.item() - batch_label[i])
        batch_grad = batch_grad.div(self.batch_size)
        batch_grad.mul_(-1.0 * self.lr)
        self.grad.add_(batch_grad)

        batch_bias = batch_bias / (len(batch_ins))
        self.bias = self.bias - batch_bias * self.lr
        return train_loss, train_acc

    def fit(self):
        num_batches = math.floor(self.num_train / self.batch_size)
        for epoch in range(self.num_epochs):
            epoch_loss = 0
            for batch_idx in range(num_batches):
                epoch_loss, epoch_acc = self.one_epoch()
            test_loss, test_acc = self.evaluate()
            print("epoch[{}]: train loss = {}, train accuracy = {}, "
                  "test loss = {}, test accuracy = {}".format(epoch, epoch_loss, epoch_acc, test_loss, test_acc))
            self.cur_index = 0
            print(self.grad.numpy().flatten())
            print(self.bias)

    def evaluate(self):
        test_loss = Loss()
        test_acc = Accuracy()
        for i in range(self.num_test):
            ins, label = self.test_set.__getitem__(i)
            z = self.forward(ins)
            h = self.sigmoid(z)
            loss = self.loss(h, label)
            # print("z= {}, h= {}, loss = {}".format(z, h, loss))
            test_loss.update(loss, 1)
            test_acc.update(h, label)
        #print(f"test set: {self.num_test}, {test_loss}, {test_acc}")
        return test_loss, test_acc


if __name__ == "__main__":
    train_file = "../dataset/agaricus_127d_train.libsvm"
    test_file = "../dataset/agaricus_127d_test.libsvm"
    train_data = SparseDatasetWithLines(train_file, 127)
    test_data = SparseDatasetWithLines(test_file, 127)
    lr = LogisticRegression(train_data, test_data, 127, 20, 0.01, 10)
    lr.fit()
