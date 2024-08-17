from src.communicator import Communicator, dynamo_primitive, dynamo_primitive_nn
from src.storage import DynamoTable


class DynamoCommunicator(Communicator):

    def __init__(self, _client, _tmp_table, _merged_table, _key_col, _num_workers, _worker_index):
        super(DynamoCommunicator, self).__init__(_client)
        self.tmp_table = _tmp_table
        self.merged_table = _merged_table
        assert isinstance(self.tmp_table, DynamoTable)
        assert isinstance(self.merged_table, DynamoTable)
        self.key_col = _key_col
        self.num_workers = _num_workers
        self.worker_index = _worker_index

    def reduce_batch(self, vector, cur_epoch=0, cur_batch=0):
        return dynamo_primitive.reduce_batch(self.tmp_table, self.merged_table, vector, self.key_col,
                                             self.num_workers, self.worker_index, cur_epoch, cur_batch)

    def reduce_batch_nn(self, weight_bytes, cur_epoch=0, cur_batch=0):
        return dynamo_primitive_nn.reduce_batch(self.tmp_table, self.merged_table, weight_bytes, self.key_col,
                                                self.num_workers, self.worker_index, cur_epoch, cur_batch)

    def reduce_epoch(self, vector, cur_epoch=0):
        return dynamo_primitive.reduce_epoch(self.tmp_table, self.merged_table, vector, self.key_col,
                                             self.num_workers, self.worker_index, cur_epoch)

    def reduce_epoch_nn(self, weight_bytes, cur_epoch=0):
        return dynamo_primitive_nn.reduce_epoch(self.tmp_table, self.merged_table, weight_bytes, self.key_col,
                                                self.num_workers, self.worker_index, cur_epoch)

    def reduce_scatter_batch(self, vector, cur_epoch=0, cur_batch=0):
        return dynamo_primitive.reduce_scatter_batch(self.tmp_table, self.merged_table, vector, self.key_col,
                                                     self.num_workers, self.worker_index, cur_epoch, cur_batch)

    def reduce_scatter_epoch(self, vector, cur_epoch=0):
        return dynamo_primitive.reduce_scatter_epoch(self.tmp_table, self.merged_table, vector, self.key_col,
                                                     self.num_workers, self.worker_index, cur_epoch)

    def delete_expired_batch(self, cur_epoch, cur_batch):
        return dynamo_primitive.delete_expired_batch(self.merged_table, self.key_col, cur_epoch, cur_batch)

    def delete_expired_epoch(self, cur_epoch):
        return dynamo_primitive.delete_expired_epoch(self.merged_table, self.key_col, cur_epoch)

    def async_reduce(self, vector, key=""):
        return dynamo_primitive.async_reduce(self.merged_table, vector, self.key_col, key)

    def async_reduce_nn(self, weight_bytes, key=""):
        return dynamo_primitive_nn.async_reduce(self.merged_table, weight_bytes, self.key_col, key)
