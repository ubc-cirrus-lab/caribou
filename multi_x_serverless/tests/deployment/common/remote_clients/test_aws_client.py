import unittest
from unittest.mock import patch

from multi_x_serverless.deployment.common.remote_client.aws_remote_client import AWSRemoteClient


class TestAWSRemoteClient(unittest.TestCase):
    @patch("boto3.session.Session")
    def setUp(self, mock_session):
        self.region = "us-west-2"
        self.aws_client = AWSRemoteClient(self.region)
        self.mock_session = mock_session

    @patch("boto3.session.Session.client")
    def test_client(self, mock_client):
        service_name = "lambda"
        self.aws_client._client(service_name)
        mock_client.assert_called_once_with(service_name)

    @patch.object(AWSRemoteClient, "_client")
    def test_get_iam_role(self, mock_client):
        role_name = "test_role"
        mock_client.return_value.get_role.return_value = {"Role": {"Arn": "arn:aws:iam::123456789012:role/test_role"}}
        result = self.aws_client.get_iam_role(role_name)
        mock_client.assert_called_once_with("iam")
        mock_client.return_value.get_role.assert_called_once_with(RoleName=role_name)
        self.assertEqual(result, "arn:aws:iam::123456789012:role/test_role")

    @patch.object(AWSRemoteClient, "_client")
    def test_get_lambda_function(self, mock_client):
        function_name = "test_function"
        mock_client.return_value.get_function.return_value = {"Configuration": {"FunctionName": "test_function"}}
        result = self.aws_client.get_lambda_function(function_name)
        mock_client.assert_called_once_with("lambda")
        mock_client.return_value.get_function.assert_called_once_with(FunctionName=function_name)
        self.assertEqual(result, {"FunctionName": "test_function"})

    @patch.object(AWSRemoteClient, "_client")
    @patch.object(AWSRemoteClient, "_create_lambda_function")
    @patch.object(AWSRemoteClient, "_wait_for_function_to_become_active")
    def test_create_function(self, mock_wait_for_function, mock_create_lambda, mock_client):
        function_name = "test_function"
        role_arn = "arn:aws:iam::123456789012:role/test_role"
        zip_contents = b"test_zip_contents"
        runtime = "python3.8"
        handler = "test_handler"
        environment_variables = {"test_key": "test_value"}
        timeout = 10
        memory_size = 128
        mock_create_lambda.return_value = ("arn:aws:lambda:us-west-2:123456789012:function:test_function", "Inactive")
        result = self.aws_client.create_function(
            function_name, role_arn, zip_contents, runtime, handler, environment_variables, timeout, memory_size
        )
        mock_create_lambda.assert_called_once()
        mock_wait_for_function.assert_called_once_with(function_name)
        self.assertEqual(result, "arn:aws:lambda:us-west-2:123456789012:function:test_function")


if __name__ == "__main__":
    unittest.main()
