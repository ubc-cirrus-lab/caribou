import torch.utils.data as data
from PIL import Image


class CIFAR10_subset(data.Dataset):

    def __init__(self, train, train_data, train_labels, test_data, test_labels, transform=None, target_transform=None):
        # self.root = os.path.expanduser(root)
        self.train = train  # training set or test set

        self.transform = transform
        self.target_transform = target_transform

        if self.train:
            self.train_data = train_data
            self.train_labels = train_labels
        else:
            self.test_data = test_data
            self.test_labels = test_labels

    def __getitem__(self, index):
        """
        Args:
            index (int): Index

        Returns:
            tuple: (image, target) where target is index of the target class.
        """
        if self.train:
            img, target = self.train_data[index], self.train_labels[index]
        else:
            img, target = self.test_data[index], self.test_labels[index]

        # doing this so that it is consistent with all other datasets
        # to return a PIL Image
        img = Image.fromarray(img)

        if self.transform is not None:
            img = self.transform(img)

        if self.target_transform is not None:
            target = self.target_transform(target)

        return img, target

    def __len__(self):
        if self.train:
            return len(self.train_data)
        else:
            return len(self.test_data)
