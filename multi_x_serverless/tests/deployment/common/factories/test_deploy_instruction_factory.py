import unittest
from multi_x_serverless.deployment.common.deploy_instructions.aws_deploy_instructions import AWSDeployInstructions
from multi_x_serverless.deployment.common.factories.deploy_instruction_factory import DeployInstructionFactory


class TestDeployInstructionFactory(unittest.TestCase):
    def test_get_deploy_instructions_aws(self):
        # Arrange
        provider = "aws"
        region = "us-west-2"
        deploy_instruction_factory = DeployInstructionFactory()

        # Act
        deploy_instructions = deploy_instruction_factory.get_deploy_instructions(provider, region)

        # Assert
        self.assertIsInstance(deploy_instructions, AWSDeployInstructions)

    def test_get_deploy_instructions_unknown(self):
        # Arrange
        provider = "unknown"
        region = "us-west-2"
        deploy_instruction_factory = DeployInstructionFactory()

        # Act & Assert
        with self.assertRaises(RuntimeError):
            deploy_instruction_factory.get_deploy_instructions(provider, region)

    def test_get_deploy_instructions_gcp(self):
        # Arrange
        provider = "gcp"
        region = "us-west-2"
        deploy_instruction_factory = DeployInstructionFactory()

        # Act & Assert
        with self.assertRaises(NotImplementedError):
            deploy_instruction_factory.get_deploy_instructions(provider, region)


if __name__ == "__main__":
    unittest.main()
