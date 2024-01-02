from typing import Sequence

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.client.deploy.models.function import Function
from multi_x_serverless.deployment.client.deploy.models.function_instance import FunctionInstance
from multi_x_serverless.deployment.client.deploy.models.instructions import Instruction
from multi_x_serverless.deployment.client.deploy.models.resource import Resource
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


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

    def __repr__(self) -> str:
        return f"""Workflow(
                name={self.name},
                resources={self._resources},
                functions={self._functions},
                edges={self._edges},
                config={self._config})
                """

    def dependencies(self) -> Sequence[Resource]:
        return self._resources

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        plans: dict[str, list[Instruction]] = {}
        if self._config is None:
            raise ValueError("Config not set, this state should not be reachable")

        for resource in self._resources:
            result = resource.get_deployment_instructions()
            if result:
                for region, instructions in result.items():
                    if region not in plans:
                        plans[region] = []
                    plans[region].extend(instructions)

        return plans

    def get_deployment_packages(self) -> list[DeploymentPackage]:
        packages: list[DeploymentPackage] = []
        for resource in self._resources:
            if isinstance(resource, Function):
                packages.append(resource.deployment_package)
        return packages

    def get_description(self) -> WorkflowConfig:
        if self._config is None:
            raise RuntimeError("Error in workflow config creation, given config is None, this should not happen")
        workflow_description = {
            "instances": [function_instance.to_json() for function_instance in self._functions],
            "start_hops": self._config.home_regions,
            "estimated_invocations_per_month": self._config.estimated_invocations_per_month,
            "constraints": self._config.constraints,
        }
        finished_instances = []
        if not isinstance(workflow_description["instances"], list):
            raise RuntimeError("Error in workflow config creation, this should not happen")
        for instance in workflow_description["instances"]:
            if not isinstance(instance, dict):
                raise RuntimeError("Error in workflow config creation, this should not happen")
            instance["succeeding_instances"] = []
            for edge in self._edges:
                if edge[0] == instance["instance_name"]:
                    instance["succeeding_instances"].append(edge[1])
            instance["preceding_instances"] = []
            for edge in self._edges:
                if edge[1] == instance["instance_name"]:
                    instance["preceding_instances"].append(edge[0])
            finished_instances.append(instance)
        workflow_description["instances"] = finished_instances

        workflow_config = WorkflowConfig(workflow_description)
        return workflow_config
