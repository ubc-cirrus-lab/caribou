import unittest
from unittest.mock import patch

from caribou.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient


class TestIntegrationTestRemoteClient(unittest.TestCase):
    @patch("caribou.common.models.remote_client.integration_test_remote_client.sqlite3")
    def test_select_all_from_table(self, mock_sqlite3):
        # Arrange
        conn_mock = mock_sqlite3.Connection.return_value
        cursor_mock = conn_mock.cursor.return_value
        expected_result = [("key1", "value1"), ("key2", "value2")]
        cursor_mock.fetchall.return_value = expected_result
        remote_client = IntegrationTestRemoteClient()

        # Act
        result = remote_client.select_all_from_table("table_name")

        # Assert
        self.assertEqual(result, expected_result)
        # pylint: disable=protected-access
        mock_sqlite3.Connection.assert_called_once_with(remote_client._db_connection())
        conn_mock.cursor.assert_called_once()  # pylint: disable=protected-access
        cursor_mock.execute.assert_called_once_with("SELECT * FROM table_name")
        cursor_mock.fetchall.assert_called_once()
        conn_mock.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
