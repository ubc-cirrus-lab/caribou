from caribou.common.provider import Provider
from caribou.deployment.common.deploy_instructions.aws_deploy_instructions import AWSDeployInstructions
from caribou.deployment.common.deploy_instructions.deploy_instructions import DeployInstructions
from caribou.deployment.common.deploy_instructions.integration_test_deploy_instruction import (
    IntegrationTestDeployInstructions,
)


class DeployInstructionFactory:
    @staticmethod
    def get_deploy_instructions(provider: str, region: str) -> DeployInstructions:
        try:
            provider_enum = Provider(provider)
        except ValueError as e:
            raise RuntimeError(f"Unknown provider {provider}") from e
        if provider_enum == Provider.AWS:
            return AWSDeployInstructions(region, provider_enum)
        if provider_enum == Provider.GCP:
            raise NotImplementedError
        if provider_enum == Provider.INTEGRATION_TEST_PROVIDER:
            return IntegrationTestDeployInstructions(region, provider_enum)
        raise RuntimeError(f"Provider not implemented: {provider}")
