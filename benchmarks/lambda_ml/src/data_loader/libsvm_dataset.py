import os
import numpy as np

import torch
from torch.utils.data.dataset import Dataset


def from_lines(lines, dim, dataset_type):
    if dataset_type == "dense_libsvm":
        return DenseDatasetWithLines(lines, dim)
    elif dataset_type == "sparse_libsvm":
        return SparseDatasetWithLines(lines, dim)
    else:
        raise Exception("dataset type {} is not supported, should be dense_libsvm or sparse_libsvm"
                        .format(dataset_type))


class SparseDatasetWithLines(Dataset):

    def __init__(self, lines, max_dim):
        self.max_dim = max_dim
        self.ins_list = []
        self.label_list = []
        for line in lines:
            line = line.strip("\n")
            ins = self.parse_line(line)
            if ins is not None:
                self.ins_list.append(ins[0])
                self.label_list.append(ins[1])
        print(f"number of instances: {len(self.ins_list)}")

    def parse_line(self, line):
        splits = line.split()
        if line is None:
            return
        if len(splits) < 1:
            return
        label = int(splits[0])
        if label == -1:
            label = 0
        indices_row = []
        indices_col = []
        values = []
        for item in splits[1:]:
            tup = item.split(":")
            indices_row.append(0)
            indices_col.append(int(tup[0])-1)
            values.append(float(tup[1]))
        i = torch.LongTensor([indices_row, indices_col])
        v = torch.FloatTensor(values)
        vector = torch.sparse.FloatTensor(i, v, torch.Size([1, self.max_dim]))
        return vector, label

    def __getitem__(self, index):
        ins = self.ins_list[index]
        label = self.label_list[index]
        return ins, label

    def __len__(self):
        return len(self.label_list)


# input is a local file path
class DenseDatasetWithFile(Dataset):

    def __init__(self, txt_path, max_dim):
        self.max_dim = max_dim
        self.ins_list = []
        self.label_list = []
        self.ins_list_np = []
        with open(txt_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                ins = self.parse_line(line)
                self.ins_list.append(ins[0])
                self.ins_list_np.append(ins[0].numpy())
                self.label_list.append(ins[1])
        self.ins_np = np.array(self.ins_list_np)
        self.label_np = np.array(self.label_list).reshape(len(self.label_list), 1)

    def parse_line(self, line):
        splits = line.split()
        label = int(splits[0])
        #values = np.zeros(self.max_dim, dtype=np.float32)
        values = [0] * self.max_dim
               
        for item in splits[1:]:
            tup = item.split(":")
           
            values[int(tup[0])] = float(tup[1])
        vector = torch.tensor(values, dtype=torch.float32)
        return vector, label

    def __getitem__(self, index):
        ins = self.ins_list[index]
        label = self.label_list[index]
        return ins, label

    def __len__(self):
        return len(self.label_list)


# input is lines
class DenseDatasetWithLines(Dataset):

    def __init__(self, lines, max_dim):
        self.max_dim = max_dim
        self.ins_list = []
        self.label_list = []
        self.ins_list_np = []
        for line in lines:
            line = line.strip("\n")
            ins = self.parse_line(line)
            if ins is not None:
                self.ins_list.append(ins[0])
                self.ins_list_np.append(ins[0].numpy())
                self.label_list.append(ins[1])
        self.ins_np = np.array(self.ins_list_np)
        self.label_np = np.array(self.label_list).reshape(len(self.label_list), 1)

    def parse_line(self, line):
        splits = line.split()
        if len(splits) >= 2:
            label = int(splits[0])
            #values = np.zeros(self.max_dim, dtype=np.float32)
            values = [0] * self.max_dim
            for item in splits[1:]:
                tup = item.split(":")
                if len(tup) == 2:
                    values[int(tup[0])-1] = float(tup[1])
                else:
                    return None
            vector = torch.tensor(values, dtype=torch.float32)
            return vector, label
        else:
            print("split line error: {}".format(line))
            return None

    def __getitem__(self, index):
        ins = self.ins_list[index]
        label = self.label_list[index]
        return ins, label

    def __len__(self):
        return len(self.label_list)


# input is numpy array
class DenseDatasetWithNP(Dataset):

    def __init__(self, max_dim, features, labels):
        self.max_dim = max_dim
        self.ins_list = []
        self.label_list = []
        for i in range(features.shape[0]):
            self.ins_list.append(self.to_tensor(features[i]))
            self.label_list.append(labels[i])

    def to_tensor(self, features):
        vector = torch.from_numpy(features).float()
        return vector

    def __getitem__(self, index):
        ins = self.ins_list[index]
        label = self.label_list[index]
        return ins, label

    def __len__(self):
        return len(self.label_list)


def main():
    train_file = "../dataset/agaricus_127d_train.libsvm"
    test_file = "../dataset/agaricus_127d_test.libsvm"
    train_data = SparseDatasetWithLines(train_file, 127)
    test_data = SparseDatasetWithLines(test_file, 127)

    print(train_data.__getitem__(0)[0])


if __name__ == '__main__':
    main()
