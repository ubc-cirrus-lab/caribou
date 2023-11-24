import unittest

from multi_x_serverless.shared.remote_logging.remote_logging import get_logger, Logger
from unittest.mock import MagicMock, patch
from pathlib import Path
import shutil
import time


class TestLogger(unittest.TestCase):

    @patch("boto3.client")
    def setUp(self, mock_boto3_client):
        self.mock_s3 = MagicMock()
        mock_boto3_client.return_value = self.mock_s3

        self.logger = Logger(component_name=__name__)
        self.filename = self.logger.log_filename

    def tearDown(self):
        parent_parent_parent_dir = Path(__file__).parent.parent.parent.parent
        logs_dir = parent_parent_parent_dir / "logs" / self.logger._Logger__component_name
        shutil.rmtree(logs_dir)

    def test_info(self):
        message = "Test info message"
        self.logger.info(message)
        self.logger.store_log()
        self.mock_s3.upload_file.assert_called_once()

    def test_error(self):
        message = "Test error message"
        self.logger.error(message)
        self.logger.store_log()
        self.mock_s3.upload_file.assert_called_once()

    def test_debug(self):
        message = "Test debug message"
        self.logger.debug(message)
        self.logger.store_log()
        self.mock_s3.upload_file.assert_called_once()

    def test_warning(self):
        message = "Test warning message"
        self.logger.warning(message)
        self.logger.store_log()
        self.mock_s3.upload_file.assert_called_once()

    def test_store_log_with_reset(self):
        pre_filename = self.logger.log_filename
        self.logger.store_log(reset_logger=True)
        self.mock_s3.connect.call_count = 1
        self.mock_s3.upload_file.call_count = 1
        self.mock_s3.upload_file.assert_called_once_with(self.filename, self.logger._Logger__bucket, self.filename.name)
        self.assertNotEqual(pre_filename, self.logger.log_filename)

    def test_connect(self):
        self.logger.connect()
        self.mock_s3.connect.call_count = 1

    def test_upload_file(self):
        self.logger.connect()
        self.logger.upload_file(self.filename, self.filename.name)
        self.mock_s3.connect.call_count = 1
        self.mock_s3.upload_file.call_count = 1
        self.mock_s3.upload_file.assert_called_once_with(self.filename, self.logger._Logger__bucket, self.filename.name)


if __name__ == "__main__":
    unittest.main()
