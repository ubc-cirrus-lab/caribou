from src.storage.dynamo import dynamo_operator
from src.storage.base import BaseStorage


class DynamoTable(BaseStorage):

    def __init__(self, _client, _table_name):
        super(DynamoTable, self).__init__()
        self.client = _client
        self.table = dynamo_operator.get_table(self.client, _table_name)

    def save(self, src_data, key="", key_col=""):
        return dynamo_operator.put_item(self.table, key_col, key, src_data)

    def load(self, key, key_col=""):
        return dynamo_operator.get_item(self.table, key_col, key)

    def load_or_wait(self, key, key_col="", sleep_time=0.1, time_out=60):
        return dynamo_operator.get_item_or_wait(self.table, key_col, key, sleep_time, time_out)

    def delete(self, key, key_col=""):
        if isinstance(key, str):
            return dynamo_operator.delete_item(self.table, key_col, key)
        elif isinstance(key, list):
            return dynamo_operator.delete_items(self.table, key_col, key)

    def clear(self, key_col=""):
        return dynamo_operator.clear_table(self.table, key_col)

    def list(self):
        return dynamo_operator.list_items(self.table)
