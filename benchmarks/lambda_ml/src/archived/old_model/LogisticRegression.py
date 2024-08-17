import torch
from torch.autograd import Variable
from torch.nn import functional as F
from torch.utils.data.sampler import SubsetRandomSampler

import numpy as np

from data_loader.libsvm_dataset import DenseDatasetWithFile


class LogisticRegression(torch.nn.Module):
    def __init__(self, _num_features, _num_classes):
        super(LogisticRegression, self).__init__()
        self.linear = torch.nn.Linear(_num_features, _num_classes)

    # torch.nn.CrossEntropyLoss includes softmax, so we don't need sigmoid here
    def forward(self, x):
        # y_pred = torch.sigmoid(self.linear(x))
        y_pred = self.linear(x)
        return y_pred


if __name__ == "__main__":
    num_features = 150
    num_classes = 2
    learning_rate = 0.1
    batch_size = 10
    num_epochs = 1
    validation_ratio = .2
    shuffle_dataset = True
    random_seed = 42

    file = "../dataset/agaricus_127d_train.libsvm"
    libsvm_dataset = DenseDatasetWithFile(file, num_features)

    # Creating data indices for training and validation splits:
    dataset_size = len(libsvm_dataset)
    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    if shuffle_dataset:
        np.random.seed(random_seed)
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating PT data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)

    train_loader = torch.utils.data.DataLoader(libsvm_dataset,
                                               batch_size=batch_size,
                                               sampler=train_sampler)
    validation_loader = torch.utils.data.DataLoader(libsvm_dataset,
                                                    batch_size=batch_size,
                                                    sampler=valid_sampler)

    model = LogisticRegression(num_features, num_classes)

    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    # Training the Model
    for epoch in range(num_epochs):
        for i, (items, labels) in enumerate(train_loader):
            items = Variable(items.view(-1, num_features))
            #items = Variable(items)
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            grad1 = model.linear.weight.grad
            grad2 = model.linear.bias.grad
            print(grad1)
            print(grad2)

            if (i + 1) % 100 == 0:
                print('Epoch: [%d/%d], Step: [%d/%d], Loss: %.4f'
                      % (epoch + 1, num_epochs, i + 1, len(train_indices) / batch_size, loss.data))

    # Test the Model
    correct = 0
    total = 0
    for items, labels in validation_loader:
        items = Variable(items.view(-1, num_features))
        #items = Variable(items)
        outputs = model(items)
        _, predicted = torch.max(outputs.data, 1)
        total += labels.size(0)
        correct += (predicted == labels).sum()

    print('Accuracy of the model on the %d test images: %d %%' % (len(val_indices), 100 * correct / total))
