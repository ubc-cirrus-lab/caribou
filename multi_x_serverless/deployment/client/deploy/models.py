from __future__ import annotations
from dataclasses import dataclass, field

from typing import Optional, Any

from multi_x_serverless.deployment.client.config import Config
from enum import Enum


@dataclass
class Instance(object):
    name: str


@dataclass
class FunctionInstance(Instance):
    resource_name: str
    entry_point: bool
    timeout: int
    memory: int
    region_group: str

    def to_json(self) -> dict:
        """
        Get the JSON representation of this function.
        """
        return {
            "name": self.name,
            "entry_point": self.entry_point,
            "timeout": self.timeout,
            "memory": self.memory,
            "region_group": self.region_group,
        }


class Resource(object):
    def __init__(self, name: str, resource_type: str) -> None:
        self._name = name
        self._resource_type = resource_type

    def name(self) -> str:
        return self._name

    def resource_type(self) -> str:
        return self._resource_type

    def dependencies(self) -> list[Resource]:
        return []

    def get_deployment_instructions(self, config: Config, endpoint: Optional[Endpoint]) -> list[Instruction]:
        return []


@dataclass
class Function(Resource):
    environment_variables: dict[str, str]
    runtime: str
    timeout: int
    memory: int
    role: IAMRole
    deployment_package: DeploymentPackage
    region_group: str

    def dependencies(self) -> list[Resource]:
        resources: list[Resource] = [self.role, self.deployment_package]
        return resources

    def get_deployment_instructions(self, config: Config, endpoint: Endpoint) -> list[Instruction]:
        if endpoint == Endpoint.AWS:
            return self.get_deployment_instructions_aws(config)
        elif endpoint == Endpoint.GCP:
            return self.get_deployment_instructions_gcp(config)
        else:
            raise Exception(f"Unknown endpoint {endpoint}")

    def get_deployment_instructions_aws(self, config: Config) -> list[Instruction]:
        pass

    def get_deployment_instructions_gcp(self, config: Config) -> list[Instruction]:
        pass


class Workflow(Resource):
    def __init__(
        self, name: str, resources: list[Function], functions: list[FunctionInstance], edges: list[tuple[str, str]]
    ) -> None:
        self._resources = resources
        self._functions = functions
        self._edges = edges

        super().__init__(name, "workflow")

    def dependencies(self) -> list[Resource]:
        return self._resources

    def add_resource(self, resource: Resource) -> None:
        self._resources.append(resource)

    def get_deployment_instructions(
        self, config: Config, endpoint: Optional[Endpoint] = None
    ) -> dict[Endpoint, list[Instruction]]:
        plans: dict[Endpoint, list[Instruction]] = []
        for resource in self._resources:
            for endpoint in Endpoint:
                if endpoint not in plans:
                    plans[endpoint] = []
                result = resource.get_deployment_instructions(config, endpoint)
                if result:
                    for instruction in result:
                        plans[endpoint].append(instruction)
        return plans


class Endpoint(Enum):
    AWS = "aws"
    GCP = "gcp"


@dataclass
class Instruction(object):
    instruction: str


@dataclass
class DeploymentPlan(object):
    instructions: dict[Endpoint, list[Instruction]] = field(default_factory=dict)


@dataclass
class IAMRole(Resource):
    resource_type: str = "iam_role"
    role_name: str
    policy: str

    def dependencies(self) -> list[Resource]:
        return [self.policy]


@dataclass
class DeploymentPackage(Resource):
    filename: Optional[str] = None
