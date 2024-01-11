from multi_x_serverless.deployment.common.deploy_instructions.aws_deploy_instructions import AWSDeployInstructions
from multi_x_serverless.deployment.common.deploy_instructions.deploy_instructions import DeployInstructions
from multi_x_serverless.deployment.common.enums import Provider


class DeployInstructionFactory:
    @staticmethod
    def get_deploy_instructions(provider: str, region: str) -> DeployInstructions:
        provider_enum = Provider(provider)
        if provider_enum == Provider.AWS:
            return AWSDeployInstructions(region)
        if provider_enum == Provider.GCP:
            raise NotImplementedError
        raise RuntimeError(f"Unknown provider {provider}")
