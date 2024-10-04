import os
import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from caribou.common import constants

from caribou.common.teardown.teardown_tables import (
    remove_table,
    remove_bucket,
    teardown_framework_tables,
    teardown_framework_buckets,
    remove_sync_tables_all_regions,
)


class TestTeardownTables(unittest.TestCase):
    @patch("boto3.client")
    def test_remove_table_exists(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb

        remove_table(mock_dynamodb, "test_table")

        mock_dynamodb.describe_table.assert_called_once_with(TableName="test_table")
        mock_dynamodb.delete_table.assert_called_once_with(TableName="test_table")

    @patch("boto3.client")
    def test_remove_table_not_exists(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_dynamodb.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "describe_table"
        )

        remove_table(mock_dynamodb, "test_table")

        mock_dynamodb.describe_table.assert_called_once_with(TableName="test_table")
        mock_dynamodb.delete_table.assert_not_called()

    @patch("boto3.client")
    def test_remove_table_other_error(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_dynamodb.describe_table.side_effect = ClientError(
            {"Error": {"Code": "SomeOtherException"}}, "describe_table"
        )

        with self.assertRaises(ClientError):
            remove_table(mock_dynamodb, "test_table")

    @patch("boto3.client")
    @patch("boto3.resource")
    def test_remove_bucket_exists(self, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource

        remove_bucket(mock_s3, mock_s3_resource, "test_bucket")

        mock_s3.head_bucket.assert_called_once_with(Bucket="test_bucket")
        mock_s3_resource.Bucket.assert_called_once_with("test_bucket")
        mock_s3_resource.Bucket().objects.all().delete.assert_called_once()
        mock_s3.delete_bucket.assert_called_once_with(Bucket="test_bucket")

    @patch("boto3.client")
    @patch("boto3.resource")
    def test_remove_bucket_not_exists(self, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource
        mock_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "head_bucket")

        remove_bucket(mock_s3, mock_s3_resource, "test_bucket")

        mock_s3.head_bucket.assert_called_once_with(Bucket="test_bucket")
        mock_s3_resource.Bucket.assert_not_called()
        mock_s3.delete_bucket.assert_not_called()

    @patch("boto3.client")
    @patch("boto3.resource")
    def test_remove_bucket_other_error(self, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource
        mock_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "SomeOtherException"}}, "head_bucket")

        with self.assertRaises(ClientError):
            remove_bucket(mock_s3, mock_s3_resource, "test_bucket")

    @patch("boto3.client")
    @patch("caribou.common.teardown.teardown_tables.constants")
    def test_teardown_framework_tables(self, mock_constants, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_constants.GLOBAL_SYSTEM_REGION = "us-west-2"
        mock_constants.SYNC_MESSAGES_TABLE = "sync_messages"
        mock_constants.SYNC_PREDECESSOR_COUNTER_TABLE = "sync_predecessor_counter"
        mock_constants.TEST_TABLE = "test_table"

        teardown_framework_tables()

        mock_dynamodb.describe_table.assert_called_once_with(TableName="test_table")
        mock_dynamodb.delete_table.assert_called_once_with(TableName="test_table")

    @patch("boto3.client")
    @patch("boto3.resource")
    @patch("caribou.common.teardown.teardown_tables.constants")
    def test_teardown_framework_buckets(self, mock_constants, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource
        mock_constants.GLOBAL_SYSTEM_REGION = "us-west-2"
        mock_constants.TEST_BUCKET = "test_bucket"

        teardown_framework_buckets()

        mock_s3.head_bucket.assert_called_once_with(Bucket="test_bucket")
        mock_s3_resource.Bucket.assert_called_once_with("test_bucket")
        mock_s3_resource.Bucket().objects.all().delete.assert_called_once()
        mock_s3.delete_bucket.assert_called_once_with(Bucket="test_bucket")

    @patch("boto3.client")
    @patch("caribou.common.teardown.teardown_tables.constants")
    @patch("caribou.common.teardown.teardown_tables.Endpoints")
    def test_remove_sync_tables_all_regions(self, mock_endpoints, mock_constants, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_constants.GLOBAL_SYSTEM_REGION = "us-west-2"
        mock_constants.SYNC_MESSAGES_TABLE = "sync_messages"
        mock_constants.SYNC_PREDECESSOR_COUNTER_TABLE = "sync_predecessor_counter"
        mock_constants.AVAILABLE_REGIONS_TABLE = "available_regions"
        mock_endpoints().get_data_collector_client().get_all_values_from_table.return_value = {
            "aws:us-east-1": {},
            "aws:us-west-1": {},
        }

        remove_sync_tables_all_regions()

        self.assertEqual(mock_boto_client.call_count, 3)
        mock_dynamodb.describe_table.assert_any_call(TableName="sync_messages")
        mock_dynamodb.describe_table.assert_any_call(TableName="sync_predecessor_counter")
        mock_dynamodb.delete_table.assert_any_call(TableName="sync_messages")
        mock_dynamodb.delete_table.assert_any_call(TableName="sync_predecessor_counter")


if __name__ == "__main__":
    unittest.main()
