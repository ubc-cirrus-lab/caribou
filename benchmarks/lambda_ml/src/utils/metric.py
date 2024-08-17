import torch


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

    def one_update(self, output, target):
        pred = 1 if output.item() >= 0 else -1

        self.correct += 1 if pred == target else 0
        self.count += 1

    def batch_update(self, output, target):
        pred = torch.sign(output)
        correct = pred.eq(target).sum().item()
        self.correct += correct
        self.count += output.size(0)


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
