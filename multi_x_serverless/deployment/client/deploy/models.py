from __future__ import annotations
from dataclasses import dataclass

from typing import Optional, Any


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
