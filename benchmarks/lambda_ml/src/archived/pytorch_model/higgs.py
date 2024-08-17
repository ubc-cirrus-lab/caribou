import torch
import torch.nn as nn
import torch.nn.functional as F


class LogisticRegression(torch.nn.Module):
    def __init__(self):
        super(LogisticRegression, self).__init__()
        self.linear = torch.nn.Linear(30, 2)

    # torch.nn.CrossEntropyLoss includes softmax, so we don't need sigmoid here
    def forward(self, x):
        # y_pred = torch.sigmoid(self.linear(x))
        y_pred = self.linear(x)
        return y_pred


class SVM(torch.nn.Module):
    def __init__(self):
        super(SVM, self).__init__()
        self.linear = torch.nn.Linear(30, 2)

    def forward(self, x):
        y_pred = self.linear(x)
        return y_pred