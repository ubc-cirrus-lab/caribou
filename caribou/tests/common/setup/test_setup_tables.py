import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from caribou.common.setup import setup_tables
from caribou.common import constants


class TestSetupTables(unittest.TestCase):
    @patch("boto3.client")
    def test_create_table_already_exists(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_dynamodb.describe_table.return_value = {"Table": {"TableName": "existing_table"}}

        with self.assertLogs(setup_tables.logger, level="INFO") as log:
            setup_tables.create_table(mock_dynamodb, "existing_table")

        mock_dynamodb.describe_table.assert_called_once_with(TableName="existing_table")
        self.assertIn("Table existing_table already exists", log.output[0])

    @patch("boto3.client")
    def test_create_table_not_exists(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_dynamodb.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "describe_table"
        )

        setup_tables.create_table(mock_dynamodb, "new_table")

        mock_dynamodb.create_table.assert_called_once_with(
            TableName="new_table",
            AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )

    @patch("boto3.client")
    def test_create_bucket_already_exists(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_bucket.return_value = {}

        with self.assertLogs(setup_tables.logger, level="INFO") as log:
            setup_tables.create_bucket(mock_s3, "existing_bucket")

        mock_s3.head_bucket.assert_called_once_with(Bucket="existing_bucket")
        self.assertIn("Bucket existing_bucket already exists", log.output[0])

    @patch("boto3.client")
    def test_create_bucket_new_bucket(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "head_bucket")

        setup_tables.create_bucket(mock_s3, "new_bucket")

        mock_s3.head_bucket.assert_called_once_with(Bucket="new_bucket")
        mock_s3.create_bucket.assert_called_once()

    @patch("boto3.client")
    def test_create_bucket_other_error(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "500"}}, "head_bucket")

        with self.assertRaises(ClientError):
            setup_tables.create_bucket(mock_s3, "error_bucket")

        mock_s3.head_bucket.assert_called_once_with(Bucket="error_bucket")
        mock_s3.create_bucket.assert_not_called()

    @patch("boto3.client")
    @patch("caribou.common.setup.setup_tables.create_table")
    def test_main_create_tables(self, mock_create_table, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb

        with patch("caribou.common.setup.setup_tables.constants") as mock_constants:
            mock_constants.GLOBAL_SYSTEM_REGION = "us-west-2"
            mock_constants.SYNC_MESSAGES_TABLE = "sync_messages_table"
            mock_constants.SYNC_PREDECESSOR_COUNTER_TABLE = "sync_predecessor_counter_table"
            mock_constants.OTHER_TABLE = "other_table"

            setup_tables.main()

            mock_create_table.assert_any_call(mock_dynamodb, "sync_messages_table")
            mock_create_table.assert_any_call(mock_dynamodb, "sync_predecessor_counter_table")
            mock_create_table.assert_any_call(mock_dynamodb, "other_table")


if __name__ == "__main__":
    unittest.main()
