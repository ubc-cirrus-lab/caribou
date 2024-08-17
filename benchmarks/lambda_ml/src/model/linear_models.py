import math

import torch
import torch.distributed as dist
import numpy as np

from src.utils.metric import Accuracy, Average


def get_model(model, n_features, n_classes):
    if model == "lr":
        return LogisticRegression(n_features, n_classes)
    elif model == "svm":
        return SVM(n_features, n_classes)
    else:
        raise Exception("algorithm {} is not supported, should be lr or svm"
                        .format(model))


def get_sparse_model(model, train_set, test_set, n_features, n_epoch, lr, batch_size):
    if model == "sparse_lr":
        return SparseLR(train_set, test_set, n_features, n_epoch, lr, batch_size)
    elif model == "sparse_svm":
        return SparseSVM(train_set, test_set, n_features, n_epoch, lr, batch_size)
    else:
        raise Exception("algorithm {} is not supported, should be sparse_lr or sparse_svm"
                        .format(model))


class LogisticRegression(torch.nn.Module):

    def __init__(self, _num_features, _num_classes):
        super(LogisticRegression, self).__init__()
        self.linear = torch.nn.Linear(_num_features, _num_classes)

    # torch.nn.CrossEntropyLoss includes softmax, don't need sigmoid here
    def forward(self, x):
        # y_pred = torch.sigmoid(self.linear(x))
        y_pred = self.linear(x)
        return y_pred


class SVM(torch.nn.Module):

    def __init__(self, _num_features, _num_classes):
        super(SVM, self).__init__()
        self.linear = torch.nn.Linear(_num_features, _num_classes)

    # torch.nn.CrossEntropyLoss includes softmax, don't need sigmoid here
    def forward(self, x):
        y_pred = self.linear(x)
        return y_pred


def dist_is_initialized():
    if dist.is_available():
        if dist.is_initialized():
            return True
    return False


class SparseLR(object):

    def __init__(self, _train_set, _test_set, _n_input, _epochs, _lr, _batch_size):
        self.train_set = _train_set
        self.test_set = _test_set
        self.num_train = self.train_set.__len__()
        self.num_test = self.test_set.__len__()
        self.n_input = _n_input
        self.weight = torch.zeros([self.n_input, 1], dtype=torch.float32, requires_grad=False)
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
        pred = torch.sparse.mm(ins, self.weight)
        pred = pred.add(self.bias)
        return pred

    def batch_forward(self, ins):
        pred = [torch.sparse.mm(i, self.weight) for i in ins]
        return torch.tensor(pred)

    def sigmoid(self, z):
        return 1 / (1 + torch.exp(-z))

    def batch_sigmoid(self, z):
        return 1 / (1 + torch.exp(-z))

    def loss(self, h, y):
        #return -y * np.log(h) - (1 - y) * np.log(1 - h)
        return -y * torch.log(h) - (1 - y) * torch.log(1 - h)

    def batch_loss(self, h, y):
        return self.loss(h, y)

    def backward(self, x, h, y):
        return x.transpose(0, 1) * (h - y)

    def batch_backward(self, x_list, h, y):
        grads = [self.backward(x_list[i], h[i], y[i]) for i in range(len(x_list))]
        return grads

    def one_batch(self, is_dist=dist_is_initialized()):
        batch_ins, batch_label = self.get_batch()
        batch_grad = torch.zeros([self.n_input, 1], dtype=torch.float32, requires_grad=False)

        train_loss = Average()
        train_acc = Accuracy()

        z = self.batch_forward(batch_ins)
        h = self.batch_sigmoid(z)
        batch_label = torch.tensor(batch_label).float()
        loss = self.batch_loss(h, batch_label)
        train_loss.update(torch.mean(loss).item(), self.batch_size)
        train_acc.batch_update(h, batch_label)
        grad_list = self.batch_backward(batch_ins, h, batch_label)
        for g in grad_list:
            batch_grad.add_(g)
        batch_grad = batch_grad.div(self.batch_size)
        batch_grad.mul_(-1.0 * self.lr)
        self.weight.add_(batch_grad)

        # for i in range(len(batch_ins)):
        #     z = self.forward(batch_ins[i])
        #     h = self.sigmoid(z)
        #     loss = self.loss(h, batch_label[i])
        #     # print("z= {}, h= {}, loss = {}".format(z, h, loss))
        #     train_loss.update(loss.data, 1)
        #     train_acc.update(h, batch_label[i])
        #     g = self.backward(batch_ins[i], h.item(), batch_label[i])
        #     batch_grad.add_(g)
        #     batch_bias += np.sum(h.item() - batch_label[i])
        # batch_grad = batch_grad.div(self.batch_size)
        # batch_grad.mul_(-1.0 * self.lr)
        # self.weight.add_(batch_grad)
        # batch_bias = batch_bias / (len(batch_ins))
        # self.bias = self.bias - batch_bias * self.lr
        return train_loss, train_acc

    def fit(self):
        num_batches = math.floor(self.num_train / self.batch_size)
        for epoch in range(self.num_epochs):
            epoch_loss = 0
            for batch_idx in range(num_batches):
                epoch_loss, epoch_acc = self.one_batch()
            test_loss, test_acc = self.evaluate()
            print("epoch[{}]: train loss = {}, train accuracy = {}, "
                  "test loss = {}, test accuracy = {}".format(epoch, epoch_loss, epoch_acc, test_loss, test_acc))
            self.cur_index = 0
            print(self.weight.numpy().flatten())
            print(self.bias)

    def evaluate(self):
        test_loss = Average()
        test_acc = Accuracy()
        for i in range(self.num_test):
            ins, label = self.test_set.__getitem__(i)
            label = 1 if label > 0 else -1
            z = self.forward(ins)
            h = self.sigmoid(z)
            loss = self.loss(h, label)
            # print("z= {}, h= {}, loss = {}".format(z, h, loss))
            test_loss.update(loss.item(), 1)
            test_acc.one_update(z, label)
        return test_loss, test_acc


class SparseSVM(object):
    """
    Based on http://eprints.pascal-network.org/archive/00004062/01/ShalevSiSr07.pdf
    """

    def __init__(self, _train_set, _test_set, _n_input, _epochs, _lr, _batch_size):
        self.train_set = _train_set
        self.test_set = _test_set
        self.num_train = self.train_set.__len__()
        self.num_test = self.test_set.__len__()
        self.n_input = _n_input
        self.weight = torch.zeros([_n_input, 1], dtype=torch.float32)
        self.bias = 0
        self.num_epochs = _epochs
        self.lr = _lr
        self.batch_size = _batch_size
        self.cur_index = 0

    def next_batch(self, batch_idx):
        start = batch_idx * self.batch_size
        end = min((batch_idx + 1) * self.batch_size, self.num_train)
        ins = [ts[0] for ts in self.train_set[start:end]]
        label = [1 if ts[1] > 0 else -1 for ts in self.train_set[start:end]]
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
        pred = torch.sparse.mm(ins, self.weight)
        return pred

    def batch_forward(self, ins):
        pred = [torch.sparse.mm(i, self.weight) for i in ins]
        return torch.tensor(pred)

    def batch_loss(self, h, y):
        # max(0, 1 - h * y)
        return torch.clamp(1 - h * y, min=0)

    def loss(self, h, y):
        # max(0, 1 - h * y)
        return torch.mean(torch.clamp(1 - h * y, min=0))

    def backward(self, loss, x, y):
        if loss == 0:
            #return torch.zeros(self.n_input, 1)
            return torch.sparse.FloatTensor(torch.LongTensor([[], []]),
                                            torch.FloatTensor([]), torch.Size([self.n_input, 1]))
        else:
            return x.transpose(0, 1) * -y

    def batch_backward(self, loss, x, y):
        grads = [self.backward(loss[i], x[i], y[i]) for i in range(len(x))]
        return grads

    def one_batch(self, is_dist=dist_is_initialized()):
        #batch_ins, batch_label = self.next_batch(batch_idx)
        batch_ins, batch_label = self.get_batch()
        batch_label = torch.tensor(batch_label).float()
        batch_grad = torch.zeros(self.n_input, 1, requires_grad=False)

        train_loss = Average()
        train_acc = Accuracy()

        h = self.batch_forward(batch_ins)
        loss = self.batch_loss(h, batch_label)
        train_loss.update(torch.mean(loss).data, self.batch_size)
        train_acc.batch_update(h, batch_label)
        grad_list = self.batch_backward(loss, batch_ins, batch_label)
        for g in grad_list:
            batch_grad.add_(g)
        batch_grad = batch_grad.div(self.batch_size)
        batch_grad.mul_(-1.0 * self.lr)
        self.weight.add_(batch_grad)

        # for i in range(len(batch_ins)):
        #     h = self.forward(batch_ins[i])
        #     loss = self.loss(h, batch_label[i])
        #     train_loss.update(loss, 1)
        #     train_acc.update(h, batch_label[i])
        #     g = self.backward(loss, batch_ins[i], batch_label[i])
        #     batch_grad.add_(g)
        #
        # batch_grad = batch_grad.div(self.batch_size)
        # batch_grad.mul_(-1.0 * self.lr)
        #self.weights.add_(batch_grad)

        return train_loss, train_acc

    def train(self):
        num_batches = math.floor(self.num_train / self.batch_size)
        for epoch in range(self.num_epochs):
            epoch_loss = 0.0
            epoch_acc = 0.0
            for batch_idx in range(num_batches):
                train_loss, train_acc = self.one_batch(batch_idx, epoch)
                epoch_loss += train_loss.average
                epoch_acc += train_acc.accuracy
            epoch_loss /= num_batches
            epoch_acc /= num_batches
            test_loss, test_acc = self.evaluate()
            print("epoch[{}]: train loss = {}, train accuracy = {}, "
                  "test loss = {}, test accuracy = {}"
                  .format(epoch, epoch_loss, epoch_acc, str(test_loss), str(test_acc)))
            self.cur_index = 0

    def evaluate(self):
        test_loss = Average()
        test_acc = Accuracy()
        for i in range(self.num_test):
            ins, label = self.test_set.__getitem__(i)
            label = 1 if label > 0 else -1
            h = torch.sparse.mm(ins, self.weight)
            loss = self.loss(h, label).data
            test_loss.update(loss, 1)
            test_acc.one_update(h, label)
        return test_loss, test_acc
