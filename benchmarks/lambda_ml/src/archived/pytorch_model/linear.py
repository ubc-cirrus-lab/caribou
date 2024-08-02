import torch
import torch.nn as nn


class LogisticRegression(torch.nn.Module):
    def __init__(self, num_features, num_classes):
        super(LogisticRegression, self).__init__()
        self.linear = torch.nn.Linear(num_features, num_classes)

    # torch.nn.CrossEntropyLoss includes softmax, so we don't need sigmoid here
    def forward(self, x):
        # y_pred = torch.sigmoid(self.linear(x))
        y_pred = self.linear(x)
        return y_pred


class SVM(torch.nn.Module):
    def __init__(self, num_features, num_classes):
        super(SVM, self).__init__()
        self.linear = torch.nn.Linear(num_features, num_classes)

    def forward(self, x):
        y_pred = self.linear(x)
        return y_pred