
class SyncMeta:

    def __init__(self, worker_index, num_workers):
        self.worker_index = worker_index
        self.num_workers = num_workers

    def __str__(self):
        return "worker id = {}, total number of workers = {}".format(self.worker_index, self.num_workers)
