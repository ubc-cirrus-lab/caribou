import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.instructions import APICall
from multi_x_serverless.deployment.common.deploy.models.remote_state import RemoteState
from multi_x_serverless.deployment.common.deploy_instructions.aws_deploy_instructions import AWSDeployInstructions
from multi_x_serverless.deployment.common.provider import Provider


class TestAWSDeployInstructions(unittest.TestCase):
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data='{"aws": {"Version": "2012-10-17"}}')
    @patch("os.path.exists", return_value=True)
    def test_get_deployment_instructions(self, mock_exists, mock_open):
        name = "test"
        role = IAMRole(role_name="test_role", policy_file="test_policy.json")
        providers = {"aws": {"config": {"memory": 128, "timeout": 3}}}
        runtime = "python3.8"
        handler = "handler"
        environment_variables = {"var": "value"}
        filename = "test.zip"
        client_mock = Mock()
        client_mock.get_predecessor_data.return_value = ['{"key": "value"}']
        with patch(
            "multi_x_serverless.deployment.common.factories.remote_client_factory.RemoteClientFactory.get_remote_client",
            return_value=client_mock,
        ):
            remote_state = RemoteState("aws", "us-west-1")
        function_exists = False
        aws_deploy_instructions = AWSDeployInstructions("us-west-1", Provider.AWS)

        instructions = aws_deploy_instructions.get_deployment_instructions(
            name, role, providers, runtime, handler, environment_variables, filename, remote_state, function_exists
        )

        self.assertIsInstance(instructions, list)
        self.assertIsInstance(instructions[0], APICall)


if __name__ == "__main__":
    unittest.main()
