import numpy as np

import torch
from torch.utils.data.dataset import Dataset


class DenseLibsvmDataset(Dataset):

    def __init__(self,
                 lines,
                 max_dim,
                 pos_tag):
        self.max_dim = max_dim
        self.pos_tag = pos_tag
        self.ins_list = []
        self.label_list = []
        for line in lines:
            line = line.strip("\n")
            ins = self.parse_line(line)
            if ins is not None:
                self.ins_list.append(ins[0].numpy())
                self.label_list.append(ins[1])

    def parse_line(self, line):
        splits = line.split()
        if len(splits) == self.max_dim + 1:
            values = [float(i) for i in splits[1:]]
            tag_splits = splits[0].split(",")
            label = 0
            if len(tag_splits) == 1:
                tag_pair = tag_splits[0].split(":")
                tag_name = tag_pair[0]
                label = 1 if tag_name == self.pos_tag else 0
            else:
                for tag_pair in tag_splits:
                    tag_name = tag_pair.split(":")[0]
                    if tag_name == self.pos_tag:
                        label = 1
                        break
            vector = torch.tensor(values, dtype=torch.float32)
            return vector, label
        else:
            print("split line error, line length {} != {}".format(len(splits), self.max_dim + 1))
            return None

    def add_more(self, lines):
        for line in lines:
            line = line.strip("\n")
            ins = self.parse_line(line)
            if ins is not None:
                self.ins_list.append(ins[0].numpy())
                self.label_list.append(ins[1])
        return

    def __getitem__(self, index):
        ins = self.ins_list[index]
        label = self.label_list[index]
        return ins, label

    def __len__(self):
        return len(self.label_list)


def main():
    train_file = "splits/0"
    lines = open(train_file).readlines()
    dataset = DenseLibsvmDataset(lines, 4096, "animal")

    totol_count = dataset.__len__()
    pos_count = 0
    for i in range(totol_count):
        if dataset.__getitem__(i)[1] == 1:
            print(dataset.__getitem__(i))
            pos_count += 1

    print("{} positive observations out of {}".format(pos_count, totol_count))


if __name__ == '__main__':
    main()
