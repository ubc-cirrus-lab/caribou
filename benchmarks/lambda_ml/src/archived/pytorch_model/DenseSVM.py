import torch
from torch.autograd import Variable
from torch.nn import functional as F
from torch.utils.data.sampler import SubsetRandomSampler

import numpy as np




class DenseSVM(torch.nn.Module):
    def __init__(self, _num_features, _num_classes):
        super(DenseSVM, self).__init__()
        self.num_class = _num_classes
        self.num_features = _num_features
        self.linear = torch.nn.Linear(_num_features, _num_classes)
    def forward(self, x):
        y_pred = self.linear(x)
        return y_pred


class MultiClassHingeLoss(torch.nn.Module):
    def __init__(margin=1):
        self.margin = margin
    def forward(self,pred,target):
        loss_value = 0
        for i in range(pred.shape[0]):
            indices = torch.arange(pred.shape[1])
            indices = indices[indices!=target[i]]
            tmp_loss = torch.index_select(pred[i,:],0, indices)-pred[i,target[i]]+self.margin
            tmp_hinge = torch.cat((torch.zeros((1)),tmp_loss))
            loss_value += torch.max(tmp_hinge,0)[0]
        return loss_value/target.shape[0]

class BinaryClassHingeLoss(torch.nn.Module):
    """
    def __init__(margin=1):
        self.margin = margin
    def forward(self,pred,target):
        target[target == 0] = -1
        loss_value = self.margin - np.mm(target.transpose,pred)
        hinge = np.cat((torch.zeros((pred.shape[0])),loss_value),1)
        return torch.mean(torch.max(hinge,1)[0])
    """
    def forward(self,input,target):
        target = torch.cat((target,target)).reshape(target.shape[0],2)
        return torch.mean(torch.clamp(1 - target * input, min=0))
