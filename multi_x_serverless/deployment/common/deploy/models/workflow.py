from typing import Any, Optional, Sequence

from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.common.deploy.models.function import Function
from multi_x_serverless.deployment.common.deploy.models.function_instance import FunctionInstance
from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction
from multi_x_serverless.deployment.common.deploy.models.resource import Resource
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class Workflow(Resource):
    def __init__(
        self,
        name: str,
        version: str,
        resources: list[Function],
        functions: list[FunctionInstance],
        edges: list[tuple[str, str]],
        config: Config,
    ) -> None:
        self._resources = resources
        self._functions = functions
        self._edges = edges
        self._config = config
        super().__init__(name, "workflow", version)

    def __repr__(self) -> str:
        return f"Workflow(name={self.name}, resources={self._resources}, functions={self._functions}, edges={self._edges}, config={self._config})"  # pylint: disable=line-too-long

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

    def get_function_description(self) -> list[dict]:
        if self._config is None:
            raise RuntimeError("Error in workflow config creation, given config is None, this should not happen")
        function_descriptions = []
        for function in self._resources:
            function_descriptions.append(function.to_json())
        return function_descriptions

    def get_deployed_regions_initial_deployment(self) -> dict[str, list[dict[str, str]]]:
        if self._config is None:
            raise RuntimeError("Error in workflow config creation, given config is None, this should not happen")
        deployed_regions = {}
        for function in self._resources:
            deployed_regions[function.name] = self._config.home_regions
        return deployed_regions

    def get_workflow_config(self) -> WorkflowConfig:
        if self._config is None:
            raise RuntimeError("Error in workflow config creation, given config is None, this should not happen")
        workflow_description = {
            "workflow_name": self.name,
            "workflow_version": self.version,
            "workflow_id": f"{self.name}-{self.version}",
            "instances": self._get_instances(),
            "start_hops": self._config.home_regions,
            # TODO (#27): Implement and incorporate Free Tier considerations into data_sources
            "estimated_invocations_per_month": self._config.estimated_invocations_per_month,
            "constraints": self._config.constraints,
            "regions_and_providers": self._config.regions_and_providers,
        }

        workflow_config = WorkflowConfig(workflow_description)
        return workflow_config

    def _get_instances(self) -> list[dict]:
        instances = [function_instance.to_json() for function_instance in self._functions]

        finished_instances = []
        for instance in instances:
            if not isinstance(instance, dict):
                raise RuntimeError("Error in workflow config creation, this should not happen")

            new_instance: dict[str, Any] = {}
            new_instance.update(instance)
            if "regions_and_providers" not in instance:
                new_instance["regions_and_providers"] = self._config.regions_and_providers
            preceding_instances = []
            succeeding_instances = []
            for edge in self._edges:
                if edge[0] == instance["instance_name"]:
                    succeeding_instances.append(edge[1])
                if edge[1] == instance["instance_name"]:
                    preceding_instances.append(edge[0])
            new_instance["succeeding_instances"] = succeeding_instances
            new_instance["preceding_instances"] = preceding_instances

            if len(instance["instance_name"].split(":")) != 3:
                raise RuntimeError("Error in workflow config creation, this should not happen")

            paths_to_sync_nodes = self._find_all_paths_to_any_sync_node(instance["instance_name"])
            predecessors_to_sync_nodes_and_sync_nodes = []
            if paths_to_sync_nodes:
                predecessors_to_sync_nodes_and_sync_nodes = [path[:-2] for path in paths_to_sync_nodes]
            new_instance["dependent_sync_predecessors"] = predecessors_to_sync_nodes_and_sync_nodes
            finished_instances.append(new_instance)
        return finished_instances

    def _find_all_paths_to_any_sync_node(
        self, start_instance: str, visited: Optional[set[str]] = None, path: Optional[list[str]] = None
    ) -> list[list[str]]:
        if visited is None:
            visited = set()
        if path is None:
            path = []
        visited.add(start_instance)
        path = path + [start_instance]
        paths: list[list[str]] = []
        for edge in self._edges:
            if edge[0] == start_instance:
                next_instance = edge[1]
                if next_instance.split(":")[1] == "sync":
                    path = path + [next_instance]
                    paths.append(path)
                elif next_instance not in visited:
                    paths.extend(self._find_all_paths_to_any_sync_node(next_instance, visited, list(path)))
        visited.remove(start_instance)
        return paths

    def _get_entry_point_instance_name(self) -> str:
        """
        Returns the name of the instance that is the entry point of the workflow.
        """
        for instance in self._functions:
            if instance.entry_point:
                return instance.name
        raise RuntimeError("No entry point instance found, this should not happen")

    def _get_workflow_placement(self, resource_values: dict[str, list[Any]]) -> dict[str, dict[str, Any]]:
        function_instance_to_identifier = self._get_function_instance_to_identifier(resource_values)

        workflow_placement = {}
        for instance in self._functions:
            workflow_placement[instance.name] = {
                "identifier": function_instance_to_identifier[instance.name],
                "provider_region": self._config.home_regions[0],  # TODO (#68): Make multi-home region workflows work
            }

        return workflow_placement

    def _extend_stage_area_placement(
        self, resource_values: dict[str, list[Any]], staging_area_placement: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        function_instance_to_identifier = self._get_function_instance_to_identifier(resource_values)

        for key in staging_area_placement["workflow_placement"].keys():
            staging_area_placement["workflow_placement"][key]["identifier"] = function_instance_to_identifier[key]

        return staging_area_placement

    def _get_function_instance_to_identifier(self, resource_values: dict[str, list[Any]]) -> dict[str, str]:
        function_resource_to_identifiers = {
            function_resource_description["name"]: function_resource_description["topic_identifier"]
            for function_resource_description in resource_values["sns_topic"]
        }

        function_instance_to_identifier = {
            function_instance.name: function_resource_to_identifiers[function_instance.function_resource_name]
            for function_instance in self._functions
        }

        return function_instance_to_identifier

    def get_workflow_placement_decision(self, resource_values: dict[str, list[Any]]) -> dict[str, Any]:
        """
        The desired output format is explained in the `docs/design.md` file under `Workflow Placement Decision`.
        """
        result: dict[str, Any] = {}

        result["instances"] = self._get_instances()
        result["current_instance_name"] = self._get_entry_point_instance_name()
        result["workflow_placement"] = self._get_workflow_placement(resource_values)
        return result

    def get_workflow_placement_decision_extend_staging(
        self,
        resource_values: dict[str, list[Any]],
        staging_area_placement: dict[str, Any],
        previous_instances: list[dict],
    ) -> dict[str, Any]:
        """
        The desired output format is explained in the `docs/design.md` file under `Workflow Placement Decision`.
        """
        staging_area_placement["instances"] = self._get_instances()
        staging_area_placement["current_instance_name"] = self._get_entry_point_instance_name()
        self._extend_stage_area_placement(resource_values, staging_area_placement)
        self._update_instances(staging_area_placement)
        return staging_area_placement

    def _update_instances(self, staging_area_placement: dict[str, Any]) -> None:
        for instance in staging_area_placement["instances"]:
            instance["function_name"] = staging_area_placement["workflow_placement"][instance["instance_name"]][
                "function_identifier"
            ].split(":")[-1]
