from src.storage import S3Storage
from src.communicator import Communicator
from src.communicator import s3_primitive, s3_primitive_nn


class S3Communicator(Communicator):

    def __init__(self, _storage, _tmp_bucket, _merged_bucket, _num_workers, _worker_index):
        super(S3Communicator, self).__init__(_storage)
        assert isinstance(self.storage, S3Storage)
        self.tmp_bucket = _tmp_bucket
        self.merged_bucket = _merged_bucket
        self.num_workers = _num_workers
        self.worker_index = _worker_index

    def reduce_batch(self, vector, postfix=""):
        return s3_primitive.reduce_batch(self.storage, vector, self.tmp_bucket, self.merged_bucket,
                                         self.num_workers, self.worker_index, postfix)

    def reduce_batch_nn(self, data_bytes, postfix=""):
        return s3_primitive_nn.reduce_batch(self.storage, data_bytes, self.tmp_bucket, self.merged_bucket,
                                            self.num_workers, self.worker_index, postfix)

    def reduce_epoch(self, vector, postfix=""):
        return s3_primitive.reduce_epoch(self.storage, vector, self.tmp_bucket, self.merged_bucket,
                                         self.num_workers, self.worker_index, postfix)

    def reduce_epoch_nn(self, data_bytes, postfix=""):
        return s3_primitive_nn.reduce_epoch(self.storage, data_bytes, self.tmp_bucket, self.merged_bucket,
                                            self.num_workers, self.worker_index, postfix)

    def reduce_scatter_batch(self, vector, postfix=""):
        return s3_primitive.reduce_scatter_batch(self.storage, vector, self.tmp_bucket, self.merged_bucket,
                                                 self.num_workers, self.worker_index, postfix)

    def reduce_scatter_epoch(self, vector, postfix=""):
        return s3_primitive.reduce_scatter_epoch(self.storage, vector, self.tmp_bucket, self.merged_bucket,
                                                 self.num_workers, self.worker_index, postfix)

    def delete_expired_batch(self, cur_epoch, cur_batch):
        return s3_primitive.delete_expired_batch(self.storage, self.merged_bucket, cur_epoch, cur_batch)

    def delete_expired_epoch(self, cur_epoch):
        return s3_primitive.delete_expired_epoch(self.storage, self.merged_bucket, cur_epoch)

    def async_reduce(self, vector, object_name=""):
        return s3_primitive.async_reduce(self.storage, vector, self.merged_bucket, object_name)

    def async_reduce_nn(self, data_bytes, object_name=""):
        return s3_primitive_nn.async_reduce(self.storage, data_bytes, self.merged_bucket, object_name)


