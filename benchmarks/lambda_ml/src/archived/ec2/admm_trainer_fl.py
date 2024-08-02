import time

import numpy as np
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


def check_stop(ep_abs, ep_rel, r, s, n, p, w, z, u, rho):
    e_pri = (n*p)**(0.5) * ep_abs + ep_rel * (max(np.sum(w**2),np.sum(n*z**2)))**(0.5)
    e_dual = (p)**(0.5) * ep_abs + ep_rel * rho * (np.sum(u**2))**(0.5)/(n)**(0.5)
    print("r^2 = {}, s^2 = {}, e_pri = {}, e_dual = {}".
          format(np.sum(r**2), e_pri, np.sum(s**2), e_dual))
    stop = (np.sum(r**2) <= e_pri**2) & (np.sum(s**2) <= e_dual**2)
    return(stop)


class ADMMTrainer(object):

    def __init__(self, model, optimizer, criterion,
                 train_loader, test_loader, lam, rho,
                 device=torch.device("cpu")):
        self.model = model
        self.optimizer = optimizer
        self.criterion = criterion
        self.train_loader = train_loader
        self.test_loader = test_loader
        self.device = device
        self.train_start = time.time()
        self.z, self.u = self.initialize_z_and_u(model.linear.weight.data.size())
        self.lam = lam
        self.rho = rho
        print("size of z = {}".format(self.z.shape))
        print("size of u = {}".format(self.u.shape))

    def initialize_z_and_u(self, shape):
        z = np.random.rand(shape[0], shape[1]).astype(np.float32)
        u = np.random.rand(shape[0], shape[1]).astype(np.float32)
        return z, u

    def update_z_u(self, w, z, u, rho, n, lam_0):
        z_new = w + u
        z_tem = abs(z_new) - lam_0 / float(n * rho)
        z_new = np.sign(z_new) * z_tem * (z_tem > 0)

        s = z_new - z
        r = w - np.ones(w.shape[0] * w.shape[1]).astype(np.float).reshape(w.shape) * z_new
        u_new = u + r
        return z_new, s, r, s

    def update_z(self, w, u, rho, n, lam_0):
        z_new = w + u
        z_tem = abs(z_new) - lam_0 / float(n * rho)
        z_new = np.sign(z_new) * z_tem * (z_tem > 0)
        return z_new

    def synchronize(self):
        size = float(dist.get_world_size())
        np_rand = np.random.uniform(0, 1)
        is_sync = 1 if np_rand > 0.5 else 0
        is_sync_tensor = torch.tensor([is_sync])
        dist.all_reduce(is_sync_tensor, op=dist.ReduceOp.SUM)
        sync_size = is_sync_tensor.item()
        print("sync rand = {}, sync = {}, syn size = {}".format(np_rand, True if is_sync == 1 else False, sync_size))
        for param in self.model.parameters():
            sync_tensor = param.grad.data.clone() if is_sync == 1 else torch.zeros_like(param.grad.data)
            dist.all_reduce(sync_tensor, op=dist.ReduceOp.SUM)
            if is_sync == 1 and sync_size >= 1:
                param.grad.data = sync_tensor / sync_size

    def fit(self, admm_epochs, epochs, is_dist=True):
        total_sync_time = 0
        np.random.seed(dist.get_rank())
        for admm_epoch in range(1, admm_epochs + 1):
            for epoch in range(1, epochs + 1):
                train_loss, train_acc = self.train(epoch, is_dist)
                test_loss, test_acc = self.evaluate()
                print(
                    'ADMM Epoch: {}/{},'.format(admm_epoch, admm_epochs),
                    'time: {} s'.format(time.time() - self.train_start),
                    'Epoch: {}/{},'.format(epoch, epochs),
                    'train loss: {}, train acc: {},'.format(train_loss, train_acc),
                    'test loss: {}, test acc: {}.'.format(test_loss, test_acc),
                )
            sync_start = time.time()
            if is_dist:
                self.synchronize()
            self.optimizer.step()

            sync_time = time.time() - sync_start
            total_sync_time += sync_time

            test_loss, test_acc = self.evaluate()
            print(
                'ADMM Epoch {}/{} finishes,'.format(admm_epoch, admm_epochs),
                'time: {} s'.format(time.time() - self.train_start),
                'train loss: {}, train acc: {},'.format(train_loss, train_acc),
                'test loss: {}, test acc: {}.'.format(test_loss, test_acc),
            )

    def train(self, epoch, is_dist=True):
        self.model.train()

        epoch_start = time.time()
        epoch_loss = 0
        num_batch = 0

        train_loss = Average()
        train_acc = Accuracy()

        for batch_idx, (data, target) in enumerate(self.train_loader):
            batch_start = time.time()

            data = data.float().to(self.device)
            target = target.to(self.device)

            # Forward + Backward + Optimize
            self.optimizer.zero_grad()
            output = self.model(data)
            classify_loss = self.criterion(output, target)
            epoch_loss += classify_loss.data
            u_z = torch.from_numpy(self.u).float() - torch.from_numpy(self.z).float()
            loss = classify_loss
            for name, param in self.model.named_parameters():
                if name.split('.')[-1] == "weight":
                    loss += self.rho / 2.0 * torch.norm(param + u_z, p=2)
            # loss = classify_loss + rho / 2.0 * torch.norm(torch.sum(model.linear.weight, u_z))
            self.optimizer.zero_grad()
            loss.backward(retain_graph=True)
            self.optimizer.step()

            num_batch += 1

            train_loss.update(loss.item(), data.size(0))
            train_acc.update(output, target)

            batch_time = time.time() - batch_start

            #progress_bar(batch_idx, len(self.train_loader), 'Loss: {} | Acc: {}'.format(train_loss, train_acc))

        epoch_time = time.time() - epoch_start

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

                loss = self.criterion(output, target)

                test_loss.update(loss.item(), data.size(0))
                test_acc.update(output, target)

        return test_loss, test_acc


print(np.random.uniform(0, 1))
print(np.random.uniform(0, 1))
print(np.random.uniform(0, 1))
