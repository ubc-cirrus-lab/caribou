from typing import Sequence

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.client.deploy.models.enums import Endpoint
from multi_x_serverless.deployment.client.deploy.models.function import Function
from multi_x_serverless.deployment.client.deploy.models.function_instance import FunctionInstance
from multi_x_serverless.deployment.client.deploy.models.instructions import APICall, Instruction
from multi_x_serverless.deployment.client.deploy.models.resource import Resource


class Workflow(Resource):
    def __init__(
        self,
        name: str,
        resources: list[Function],
        functions: list[FunctionInstance],
        edges: list[tuple[str, str]],
        config: Config,
    ) -> None:
        self._resources = resources
        self._functions = functions
        self._edges = edges
        super().__init__(name, "workflow", config=config)

    def dependencies(self) -> Sequence[Resource]:
        return self._resources

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        plans: dict[str, list[Instruction]] = {}
        if self._config is None:
            raise ValueError("Config not set, this state should not be reachable")
        for home_region in self._config.home_regions:
            endpoint, region = home_region.split(":")
            if endpoint == Endpoint.AWS.value:
                if home_region not in plans:
                    plans[home_region] = []
                plans[home_region].append(self.get_sns_topic_instruction_for_region(region))
            elif endpoint == Endpoint.GCP.value:
                raise NotImplementedError()

        for resource in self._resources:
            result = resource.get_deployment_instructions()
            if result:
                for region, instructions in result.items():
                    plans[region].extend(instructions)

        return plans

    def get_sns_topic_instruction_for_region(self, region: str) -> Instruction:
        return APICall(
            name="create_sns_topic",
            params={
                "topic_name": f"{self.name}_{region}",
            },
            output_var=f"{self.name}_{region}_sns_topic_arn",
        )

    def get_deployment_packages(self) -> list[DeploymentPackage]:
        packages: list[DeploymentPackage] = []
        for resource in self._resources:
            if isinstance(resource, Function):
                packages.append(resource.deployment_package)
        return packages
