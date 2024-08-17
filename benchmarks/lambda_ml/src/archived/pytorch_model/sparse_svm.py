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
        pred = 1 if output.item() >= 0 else -1

        self.correct += 1 if pred == target else 0
        self.count += 1

    def batch_update(self, output, target):
        pred = torch.sign(output)
        correct = pred.eq(target).sum().item()
        self.correct += correct
        self.count += output.size(0)


class SparseSVM(object):
    """ Based on http://eprints.pascal-network.org/archive/00004062/01/ShalevSiSr07.pdf
    """

    def __init__(self, _train_set, _test_set, _n_input, _epochs, _lr, _batch_size):
        self.train_set = _train_set
        self.test_set = _test_set
        self.num_train = self.train_set.__len__()
        self.num_test = self.test_set.__len__()
        self.n_input = _n_input
        self.weights = torch.zeros(_n_input, 1)
        self.num_epochs = _epochs
        self.lr = _lr
        self.batch_size = _batch_size
        self.cur_index = 0

    def compute_cost(self, W, X, Y):
        # calculate hinge loss
        N = X.shape[0]
        distances = 1 - Y * (torch.sparse.mm(X, W))
        distances[distances < 0] = 0  # equivalent to max(0, distance)
        hinge_loss = 10000 * (np.sum(distances) / N)

        # calculate cost
        cost = 1 / 2 * np.dot(W, W) + hinge_loss
        return cost

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

    def one_epoch(self, batch_idx, iteration, is_dist=dist_is_initialized()):
        batch_ins, batch_label = self.next_batch(batch_idx)

        train_acc = Accuracy()
        eta = 1.0 / (self.lr * (1 + iteration))

        for i in range(len(batch_ins)):
            ascent = batch_label[i] * float(torch.sparse.mm(batch_ins[i], self.weights))
            if (ascent < 1.0 and batch_label[i] != 0.0):
                self.weights = (1 - self.lr * eta) * self.weights + eta * batch_label[i] * batch_ins[i].t()
            else:
                self.weights = (1 - self.lr * eta) * self.weights
            prediction = torch.sparse.mm(batch_ins[i], self.weights)
            # print(loss)
            train_acc.update(prediction, batch_label[i])
        # print(f"train_acc:{train_acc}")
        return train_acc

    def train(self):
        num_batches = math.floor(self.num_train / self.batch_size)
        for epoch in range(self.num_epochs):
            epoch_loss = 0
            for batch_idx in range(num_batches):
                epoch_acc = self.one_epoch(batch_idx, epoch)
            test_acc = self.evaluate()
            print("epoch[{}]: train accuracy = {}, "
                  "test accuracy = {}".format(epoch, epoch_acc, test_acc))
            self.cur_index = 0

    def evaluate(self):
        # test_loss = Loss()
        test_acc = Accuracy()
        for i in range(self.num_test):
            ins, label = self.test_set.__getitem__(i)
            label = 1 if label > 0 else -1
            y = torch.sparse.mm(ins, self.weights)
            # loss = self.loss(y, label)
            # test_loss.update(loss, 1)
            test_acc.update(y, label)
        return test_acc


class SparseSVM2(object):
    """ Based on http://eprints.pascal-network.org/archive/00004062/01/ShalevSiSr07.pdf
    """

    def __init__(self, _train_set, _test_set, _n_input, _epochs, _lr, _batch_size):
        self.train_set = _train_set
        self.test_set = _test_set
        self.num_train = self.train_set.__len__()
        self.num_test = self.test_set.__len__()
        self.n_input = _n_input
        self.weights = torch.zeros(_n_input, 1)
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
        pred = torch.sparse.mm(ins, self.weights)
        return pred

    def batch_forward(self, ins):
        pred = [torch.sparse.mm(i, self.weights) for i in ins]
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

    def one_batch(self, batch_idx, iteration, is_dist=dist_is_initialized()):
        batch_ins, batch_label = self.next_batch(batch_idx)
        batch_label = torch.tensor(batch_label).float()
        batch_grad = torch.zeros(self.n_input, 1, requires_grad=False)

        train_loss = Loss()
        train_acc = Accuracy()

        h = self.batch_forward(batch_ins)
        loss = self.batch_loss(h, batch_label)
        train_loss.update(torch.mean(loss), self.batch_size)
        train_acc.batch_update(h, batch_label)
        g_list = self.batch_backward(loss, batch_ins, batch_label)
        for g in g_list:
            batch_grad.add_(g)
        batch_grad.mul_(-1.0 * self.lr)
        self.weights.add_(batch_grad)

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
        test_loss = Loss()
        test_acc = Accuracy()
        for i in range(self.num_test):
            ins, label = self.test_set.__getitem__(i)
            label = 1 if label > 0 else -1
            h = torch.sparse.mm(ins, self.weights)
            loss = self.loss(h, label)
            test_loss.update(loss, 1)
            test_acc.update(h, label)
        return test_loss, test_acc


if __name__ == "__main__":
    train_file = "../dataset/agaricus_127d_train.libsvm"
    test_file = "../dataset/agaricus_127d_test.libsvm"
    dataset = SparseDatasetWithLines(open(train_file).readlines(), 127)
    dataset_size = len(dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(0.1 * dataset_size))
    np.random.seed(42)
    np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    train_set = [dataset[i] for i in train_indices]
    val_set = [dataset[i] for i in val_indices]

    svm = SparseSVM2(train_set, val_set, 127, 20, 0.01, 10)
    svm.train()
