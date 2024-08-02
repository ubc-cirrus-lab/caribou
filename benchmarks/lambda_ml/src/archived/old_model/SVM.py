import torch
from torch.nn import functional as F


class SVM(torch.nn.Module):
    def __init__(self, _num_features, _num_classes):
        super(SVM, self).__init__()
        self.linear = torch.nn.Linear(_num_features, _num_classes)

    # torch.nn.CrossEntropyLoss includes softmax, so we don't need sigmoid here
    def forward(self, x):
        y_pred = self.linear(x)
        return y_pred
