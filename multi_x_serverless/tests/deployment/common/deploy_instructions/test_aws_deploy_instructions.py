import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.instructions import APICall, RecordResourceVariable
from multi_x_serverless.deployment.common.deploy.models.remote_state import RemoteState
from multi_x_serverless.deployment.common.deploy_instructions.aws_deploy_instructions import AWSDeployInstructions
from multi_x_serverless.common.provider import Provider


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
        client_mock.resource_exists.return_value = False
        with patch(
            "multi_x_serverless.deployment.common.factories.remote_client_factory.RemoteClientFactory.get_remote_client",
            return_value=client_mock,
        ):
            remote_state = RemoteState("aws", "region1")
        function_exists = False
        aws_deploy_instructions = AWSDeployInstructions("region1", Provider.AWS)

        instructions = aws_deploy_instructions.get_deployment_instructions(
            name, role, providers, runtime, handler, environment_variables, filename, remote_state, function_exists
        )

        self.assertIsInstance(instructions, list)
        self.assertIsInstance(instructions[0], APICall)
        self.assertEqual(instructions[0].name, "create_sns_topic")
        self.assertEqual(instructions[0].params["topic_name"], "test_region1_sns_topic")
        self.assertEqual(instructions[0].output_var, "test_region1_sns_topic")

        self.assertIsInstance(instructions[1], RecordResourceVariable)
        self.assertEqual(instructions[1].name, "topic_identifier")
        self.assertEqual(instructions[1].variable_name, "test_region1_sns_topic")
        self.assertEqual(instructions[1].resource_name, "test")
        self.assertEqual(instructions[1].resource_type, "messaging_topic")

        self.assertIsInstance(instructions[2], APICall)
        self.assertEqual(instructions[2].name, "create_role")

        self.assertIsInstance(instructions[3], RecordResourceVariable)
        self.assertEqual(instructions[3].name, "role_identifier")

        self.assertIsInstance(instructions[4], APICall)
        self.assertEqual(instructions[4].name, "create_function")

        self.assertIsInstance(instructions[5], RecordResourceVariable)
        self.assertEqual(instructions[5].name, "function_identifier")


if __name__ == "__main__":
    unittest.main()
