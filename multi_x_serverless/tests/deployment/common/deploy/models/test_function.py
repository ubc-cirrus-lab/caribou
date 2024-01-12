import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.common.deploy.models.function import Function
from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction
from multi_x_serverless.deployment.common.deploy.models.remote_state import RemoteState
from multi_x_serverless.deployment.common.factories.deploy_instruction_factory import DeployInstructionFactory


class TestFunction(unittest.TestCase):
    def setUp(self):
        self.role = IAMRole('{"test_policy": "test"}', "test_role")
        self.deployment_package = DeploymentPackage("package_name")
        self.function = Function(
            "function_name",
            True,
            self.role,
            self.deployment_package,
            {"env_var": "value"},
            "handler",
            "runtime",
            [{"provider": "aws", "region": "us-west-1"}],
            {"aws": {}},
        )

    def test_initialise_remote_states(self):
        self.assertEqual(len(self.function._remote_states), 1)
        self.assertIsInstance(self.function._remote_states["aws"]["us-west-1"], RemoteState)

    def test_dependencies(self):
        dependencies = self.function.dependencies()
        self.assertEqual(dependencies, [self.role, self.deployment_package])

    def test_get_deployment_instructions(self):
        with patch(
            "multi_x_serverless.deployment.common.factories.deploy_instruction_factory.DeployInstructionFactory.get_deploy_instructions"
        ) as mock_get_deploy_instructions:
            mock_deploy_instruction = Mock()
            mock_get_deploy_instructions.return_value = mock_deploy_instruction
            mock_deploy_instruction.get_deployment_instructions.return_value = [Instruction("Some instruction")]

            self.deployment_package.filename = "filename"
            instructions = self.function.get_deployment_instructions()

            mock_get_deploy_instructions.assert_called_once_with("aws", "us-west-1")
            mock_deploy_instruction.get_deployment_instructions.assert_called_once_with(
                self.function.name,
                self.function.role,
                self.function.providers,
                self.function.runtime,
                self.function.handler,
                self.function.environment_variables,
                self.function.deployment_package.filename,
                self.function._remote_states["aws"]["us-west-1"],
                self.function._remote_states["aws"]["us-west-1"].resource_exists(self.function),
            )
            self.assertEqual(instructions, {"aws:us-west-1": [Instruction("Some instruction")]})

    def test_get_deployment_instructions_deployment_package_not_built(self):
        with patch(
            "multi_x_serverless.deployment.common.factories.deploy_instruction_factory.DeployInstructionFactory.get_deploy_instructions"
        ) as mock_get_deploy_instructions:
            mock_deploy_instruction = Mock()
            mock_get_deploy_instructions.return_value = mock_deploy_instruction
            mock_deploy_instruction.get_deployment_instructions.return_value = [Instruction("Some instruction")]

            self.deployment_package.filename = None
            with self.assertRaises(RuntimeError):
                _ = self.function.get_deployment_instructions()

    def test_get_deployment_instructions_gcp(self):
        with self.assertRaises(NotImplementedError):
            self.function.get_deployment_instructions_gcp("us-west-1")


if __name__ == "__main__":
    unittest.main()
