from abc import ABC, abstractmethod
from typing import Union, Iterator, List

import random

class HyperSpace(object):

    @abstractmethod
    def __init__(self) -> None:
        """TODO"""

    @abstractmethod
    def sample(self) -> Union[float, str]:
        """TODO"""

class ContHyper(HyperSpace):

    def __init__(self, name, lower, upper):
        self.name = name
        self.lower = lower
        self.upper = upper

    def sample(self):
        return self.lower + random.random() * (self.upper - self.lower)


class DiscHyper(HyperSpace):

    def __init__(self, name, values):
        self.name = name
        self.values = values
        self.cur_idx = -1

    def sample(self):
        idx = random.randint(0, len(self.values))
        return self.values[idx]

    def next(self):
        self.cur_idx += 1
        return self.values[self.cur_idx]


class CateHyper(HyperSpace):

    def __init__(self, name, values):
        self.name = name
        self.values = values
        self.cur_idx = -1

    def sample(self):
        idx = random.randint(0, len(self.values))
        return self.values[idx]

    def next(self):
        self.cur_idx += 1
        return self.values[self.cur_idx]


if __name__ == '__main__':
    cont_hyper = ContHyper("cont_hyper", 1, 10)
    for i in range(10):
        print(cont_hyper.sample())

    disc_hyper = DiscHyper("disc_hyper", range(10))
    for i in range(10):
        print(disc_hyper.sample(), disc_hyper.next())

    cate_hyper = CateHyper("cate_hyper", ["a", "b", "c", "d", "e"])
    for i in range(10):
        print(cate_hyper.sample(), cate_hyper.next())
