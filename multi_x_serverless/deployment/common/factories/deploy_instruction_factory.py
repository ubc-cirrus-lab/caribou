from multi_x_serverless.deployment.common.deploy_instructions.aws_deploy_instructions import AWSDeployInstructions
from multi_x_serverless.deployment.common.deploy_instructions.deploy_instructions import DeployInstructions
from multi_x_serverless.common.provider import Provider


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
        raise RuntimeError(f"Provider not implemented: {provider}")
