import unittest
from unittest.mock import patch, Mock
import botocore.exceptions
import multi_x_serverless.deployment.client.multi_x_serverless as multi_x_serverless

class TestMultiXServerless(unittest.TestCase):
    @patch('multi_x_serverless.deployment.client.multi_x_serverless.cli', return_value=0)
    def test_main_no_exceptions(self, mock_cli):
        self.assertEqual(multi_x_serverless.main(), 0)

    @patch('multi_x_serverless.deployment.client.multi_x_serverless.cli', side_effect=botocore.exceptions.NoRegionError)
    @patch('click.echo')
    def test_main_no_region_error(self, mock_echo, mock_cli):
        self.assertEqual(multi_x_serverless.main(), 2)
        mock_echo.assert_called_once_with("No region specified. Please specify a region in your AWS config file.", err=True)

    @patch('multi_x_serverless.deployment.client.multi_x_serverless.cli', side_effect=Exception)
    @patch('click.echo')
    def test_main_general_exception(self, mock_echo, mock_cli):
        self.assertEqual(multi_x_serverless.main(), 2)
        self.assertTrue(mock_echo.called)

if __name__ == '__main__':
    unittest.main()