import unittest
from unittest.mock import Mock, patch
from caribou.deployment.common.deploy.models.function import Function
from caribou.deployment.common.deploy.models.iam_role import IAMRole
from caribou.deployment.common.deploy.models.deployment_package import DeploymentPackage
from caribou.deployment.common.deploy.models.instructions import Instruction
from caribou.deployment.common.deploy.models.remote_state import RemoteState
from caribou.deployment.common.factories.deploy_instruction_factory import DeployInstructionFactory


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
            {"provider": "provider1", "region": "region1"},
            {"provider1": {}},
        )

    def test_initialise_remote_states(self):
        self.assertIsInstance(self.function._remote_state, RemoteState)

    def test_dependencies(self):
        dependencies = self.function.dependencies()
        self.assertEqual(dependencies, [self.role, self.deployment_package])

    def test_get_deployment_instructions(self):
        with patch(
            "caribou.deployment.common.factories.deploy_instruction_factory.DeployInstructionFactory.get_deploy_instructions"
        ) as mock_get_deploy_instructions, patch(
            "caribou.deployment.common.deploy.models.resource.Resource"
        ) as MockResource:
            mock_deploy_instruction = Mock()
            mock_get_deploy_instructions.return_value = mock_deploy_instruction
            mock_deploy_instruction.get_deployment_instructions.return_value = [Instruction("Some instruction")]

            mock_resource = MockResource()
            mock_resource.resource_exists.return_value = False

            self.function._remote_state = mock_resource
            self.deployment_package.filename = "filename"
            instructions = self.function.get_deployment_instructions()

            mock_get_deploy_instructions.assert_called_once_with("provider1", "region1")
            mock_deploy_instruction.get_deployment_instructions.assert_called_once_with(
                self.function.name,
                self.function.role,
                self.function.providers,
                self.function.runtime,
                self.function.handler,
                self.function.environment_variables,
                self.function.deployment_package.filename,
                self.function._remote_state,
                False,
            )
            self.assertEqual(instructions, {"provider1:region1": [Instruction("Some instruction")]})

    def test_get_deployment_instructions_deployment_package_not_built(self):
        with patch(
            "caribou.deployment.common.factories.deploy_instruction_factory.DeployInstructionFactory.get_deploy_instructions"
        ) as mock_get_deploy_instructions:
            mock_deploy_instruction = Mock()
            mock_get_deploy_instructions.return_value = mock_deploy_instruction
            mock_deploy_instruction.get_deployment_instructions.return_value = [Instruction("Some instruction")]

            self.deployment_package.filename = None
            with self.assertRaises(RuntimeError):
                _ = self.function.get_deployment_instructions()

    def test_get_deployment_instructions_gcp(self):
        with self.assertRaises(NotImplementedError):
            self.function.get_deployment_instructions_gcp("region1")


if __name__ == "__main__":
    unittest.main()
