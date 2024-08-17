import time
import numpy as np


class ModelPool(object):

    def __init__(self):
        self.models = {}
        self.model_ts = {}

    def register(self, mid, length):
        self.models[mid] = np.zeros(length)
        self.model_ts[mid] = time.time()

    def delete(self, mid):
        self.models.pop(mid)
        self.model_ts.pop(mid)

    def get(self, mid):
        self.models[mid]

    def add(self, mid, update):
        assert self.models[mid].shape[0] == len(update)
        self.models[mid] += np.asarray(update)

    def axpy(self, mid, update, alpha):
        assert self.models[mid].shape[0] == len(update)
        delta = np.asarray(update) * alpha
        self.models[mid] += delta
