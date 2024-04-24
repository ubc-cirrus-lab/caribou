import unittest
from unittest.mock import patch, Mock
import botocore.exceptions
import caribou.deployment.client.caribou as caribou


class TestCaribou(unittest.TestCase):
    @patch("caribou.deployment.client.caribou.cli", return_value=0)
    def test_main_no_exceptions(self, mock_cli):
        self.assertEqual(caribou.main(), 0)

    @patch("caribou.deployment.client.caribou.cli", side_effect=botocore.exceptions.NoRegionError)
    @patch("click.echo")
    def test_main_no_region_error(self, mock_echo, mock_cli):
        self.assertEqual(caribou.main(), 2)
        mock_echo.assert_called_once_with(
            "No region specified. Please specify a region in your AWS config file.", err=True
        )

    @patch("caribou.deployment.client.caribou.cli", side_effect=Exception)
    @patch("click.echo")
    def test_main_general_exception(self, mock_echo, mock_cli):
        self.assertEqual(caribou.main(), 2)
        self.assertTrue(mock_echo.called)


if __name__ == "__main__":
    unittest.main()
