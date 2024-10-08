import re
from typing import Any, Optional, Sequence

from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.models.deployment_package import DeploymentPackage
from caribou.deployment.common.deploy.models.function import Function
from caribou.deployment.common.deploy.models.function_instance import FunctionInstance
from caribou.deployment.common.deploy.models.instructions import Instruction
from caribou.deployment.common.deploy.models.resource import Resource
from caribou.deployment_solver.workflow_config import WorkflowConfig


class Workflow(Resource):
    def __init__(
        self,
        name: str,
        version: str,
        resources: list[Function],
        functions: list[FunctionInstance],
        edges: list[tuple[str, str]],
        config: Config,
        allow_no_deployment_package: bool = False,
    ) -> None:
        self._resources = resources
        self._functions = functions
        self._edges = edges
        self._config = config
        self._deployed_regions: dict[str, dict[str, str]] = {}
        self.allow_no_deployment_package = allow_no_deployment_package
        super().__init__(name, "workflow", version)

    def __repr__(self) -> str:
        return f"Workflow(name={self.name}, resources={self._resources}, functions={self._functions}, edges={self._edges}, config={self._config})"  # pylint: disable=line-too-long

    def dependencies(self) -> Sequence[Resource]:
        return self._resources

    def set_deployed_regions(self, deployed_regions: dict[str, dict[str, str]]) -> None:
        self._deployed_regions = deployed_regions

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        plans: dict[str, list[Instruction]] = {}
        if self._config is None:
            raise ValueError("Config not set, this state should not be reachable")

        for resource in self._resources:
            if resource.deploy:
                resource.allow_no_deployment_package = self.allow_no_deployment_package
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

    def get_deployed_regions_initial_deployment(
        self, resource_values: dict[str, list[Any]]
    ) -> dict[str, dict[str, Any]]:
        if self._config is None:
            raise RuntimeError("Error in workflow config creation, given config is None, this should not happen")

        function_resource_to_messaging_identifier = self._get_function_resource_to_identifier(
            resource_values["messaging_topic"], "topic_identifier"
        )
        function_resource_to_function_identifier = self._get_function_resource_to_identifier(
            resource_values["function"], "function_identifier"
        )

        deployed_regions: dict[str, dict[str, Any]] = {}
        for function in self._resources:
            deployed_regions[function.name] = {
                "deploy_region": function.deploy_region,
                "message_topic": function_resource_to_messaging_identifier[function.name],
                "function_identifier": function_resource_to_function_identifier[function.name],
            }
        return deployed_regions

    def update_deployed_regions(
        self, resource_values: dict[str, list[Any]], deployed_regions: dict[str, dict[str, Any]]
    ) -> None:
        if self._config is None:
            raise RuntimeError("Error in workflow config creation, given config is None, this should not happen")

        function_resource_to_messaging_identifier = None
        function_resource_to_function_identifier = None
        if "messaging_topic" in resource_values and "function" in resource_values:
            function_resource_to_messaging_identifier = self._get_function_resource_to_identifier(
                resource_values["messaging_topic"], "topic_identifier"
            )
            function_resource_to_function_identifier = self._get_function_resource_to_identifier(
                resource_values["function"], "function_identifier"
            )

        for function in self._resources:
            if function.name not in deployed_regions:
                if function_resource_to_messaging_identifier and function_resource_to_function_identifier:
                    deployed_regions[function.name] = {
                        "deploy_region": function.deploy_region,
                        "message_topic": function_resource_to_messaging_identifier[function.name],
                        "function_identifier": function_resource_to_function_identifier[function.name],
                    }
                else:
                    raise RuntimeError("Error in workflow config creation, this should not happen")
        self._deployed_regions = deployed_regions

    def get_workflow_config(self) -> WorkflowConfig:
        if self._config is None:
            raise RuntimeError("Error in workflow config creation, given config is None, this should not happen")
        workflow_description = {
            "workflow_name": self.name,
            "workflow_version": self.version,
            "workflow_id": f"{self.name}-{self.version}",
            "instances": self._get_instances(),
            "home_region": self._config.home_region,
            "estimated_invocations_per_month": self._config.estimated_invocations_per_month,
            "constraints": self._config.constraints,
            "regions_and_providers": self._config.regions_and_providers,
            "num_calls_in_one_month": self._config.num_calls_in_one_month,
            "solver": self._config.solver,
        }

        workflow_config = WorkflowConfig(workflow_description)
        return workflow_config

    def _get_instances(self) -> dict[str, dict[str, Any]]:
        instances = {function_instance.name: function_instance.to_json() for function_instance in self._functions}

        for instance in instances.values():
            self._get_instance(instance)
        return instances

    def _get_instance(self, instance: dict[str, Any]) -> None:
        if not isinstance(instance, dict):
            raise RuntimeError("Error in workflow config creation, this should not happen")

        if "regions_and_providers" not in instance:
            instance["regions_and_providers"] = self._config.regions_and_providers
        preceding_instances = []
        succeeding_instances = []
        for edge in self._edges:
            if edge[0] == instance["instance_name"]:
                succeeding_instances.append(edge[1])
            if edge[1] == instance["instance_name"]:
                preceding_instances.append(edge[0])
        instance["succeeding_instances"] = succeeding_instances
        instance["preceding_instances"] = preceding_instances

        if len(instance["instance_name"].split(":")) != 3:
            raise RuntimeError("Error in workflow config creation, this should not happen")

        paths_to_sync_nodes = self._find_all_paths_to_any_sync_node(instance["instance_name"])
        predecessors_to_sync_nodes_and_sync_nodes = []
        if paths_to_sync_nodes:
            predecessors_to_sync_nodes_and_sync_nodes = [path[-2:] for path in paths_to_sync_nodes]
        instance["dependent_sync_predecessors"] = predecessors_to_sync_nodes_and_sync_nodes

    def _find_all_paths_to_any_sync_node(
        self, start_instance: str, visited: Optional[set[str]] = None, path: Optional[list[str]] = None
    ) -> list[list[str]]:
        if visited is None:
            visited = set()
        if path is None:
            path = []
        edge_visited = set()
        visited.add(start_instance)
        path = path + [start_instance]
        paths: list[list[str]] = []
        for edge in self._edges:
            if edge in edge_visited:
                continue
            if edge[0] == start_instance:
                next_instance = edge[1]
                if next_instance.split(":")[1] == "sync":
                    path = path + [next_instance]
                    paths.append(path)
                elif next_instance not in visited:
                    paths.extend(self._find_all_paths_to_any_sync_node(next_instance, visited, list(path)))
            edge_visited.add(edge)
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

    def _get_workflow_placement(self) -> dict[str, dict[str, Any]]:
        workflow_placement_instances = {}
        for instance in self._functions:
            workflow_placement_instances[instance.name] = {
                "identifier": self._deployed_regions[instance.function_resource_name]["message_topic"],
                "provider_region": self._config.home_region,
                "function_identifier": self._deployed_regions[instance.function_resource_name]["function_identifier"],
            }
        return workflow_placement_instances

    def get_deployment_instances(self, staging_area_placement: dict[str, Any]) -> dict[str, dict[str, Any]]:
        deployment_instances = {}
        function_instance_to_resource_name = self._get_function_instance_to_resource_name(staging_area_placement)

        for instance_name, instance in staging_area_placement.items():
            instance["identifier"] = self._deployed_regions[function_instance_to_resource_name[instance_name]][
                "message_topic"
            ]
            instance["function_identifier"] = self._deployed_regions[function_instance_to_resource_name[instance_name]][
                "function_identifier"
            ]
            deployment_instances[instance_name] = instance

        return deployment_instances

    def _get_function_instance_to_resource_name(self, staging_area_placement: dict[str, Any]) -> dict[str, str]:
        function_instance_to_resource_name = {}
        for instance_name, placement in staging_area_placement.items():
            function_name = instance_name.split(":")[0]

            provider_region = placement["provider_region"]

            function_resource_name = function_name + "_" + provider_region["provider"] + "-" + provider_region["region"]

            function_instance_to_resource_name[instance_name] = function_resource_name

        return function_instance_to_resource_name

    def _get_function_resource_to_identifier(self, resource_values: list[Any], identifier_key: str) -> dict[str, str]:
        function_resource_to_identifiers = {
            function_resource_description["name"]: function_resource_description[identifier_key]
            for function_resource_description in resource_values
        }

        return function_resource_to_identifiers

    def get_workflow_placement_decision_initial_deployment(self) -> dict[str, Any]:
        """
        The desired output format is explained in the `docs/design.md` file under `Workflow Placement Decision`.
        """
        result: dict[str, Any] = {}

        result["instances"] = self._get_instances()
        result["current_instance_name"] = self._get_entry_point_instance_name()

        result["workflow_placement"] = {}
        result["workflow_placement"]["home_deployment"] = self._get_workflow_placement()

        return result

    def _get_entry_point_from_previous_instances(self, previous_instances: dict[str, Any]) -> str:
        for instance in previous_instances.values():
            if "instance_name" in instance and instance["instance_name"].split(":")[1] == "entry_point":
                return instance["instance_name"]
        raise RuntimeError("No entry point instance found, this should not happen")

    def verify_name_and_version(self) -> None:
        version_name = self.version
        if version_name is None:
            raise RuntimeError("Workflow version is not set")

        # Version name should also be less than or equal to 10 characters and not empty
        if len(version_name) > 10 or len(version_name) == 0:
            raise RuntimeError("Workflow version must be greater than 0 and less than or equal to 10 characters")

        # Ensure that version name must only contains numbers or dots
        if not version_name.replace(".", "").isdigit():
            raise RuntimeError("Workflow version must contain only numbers or dots")

        # Ensure the length of the workflow name is less than or equal to 25 characters and not empty
        if len(self.name) > 25 or len(self.name) == 0:
            raise RuntimeError("Workflow name must be greater than 0 and less than or equal to 25 characters")

        # Ensure that the workflow name must contain only letters, numbers, or underscores
        if not self.name.replace("_", "").isalnum():
            raise RuntimeError("Workflow name must contain only letters, numbers, or underscores")

        # Ensure the name of the function must contain only letters, numbers, or underscores
        # Also put a limit on the length of the function name to be less than or equal to 15 characters
        for function in self._resources:
            function_name = function.name

            # First check the overall length of the function name
            if len(function_name) > 64:
                raise RuntimeError(
                    f"AWS Lambda Function name must be less than or equal to 64 characters,"
                    f"please shorten the workflow name, version, and or function name. \n"
                    f"Current Caribou Full function name: {function_name} \n"
                    f"Current length: {len(function_name)} characters"
                )

            # Define the regular expression pattern to match the function name
            # (Isolate the actual function name from the full function name)
            # NOTE: This may need to be changed if there are major changes to the
            # naming convention of the function name in _get_function_name() method
            # of wokflow_builder.py
            pattern = r"^[^-]+-[^-]+-(.*)_[^-]+-"

            # Search for the pattern in the input string
            match = re.search(pattern, function_name)
            if match:
                function_name = match.group(1)
            else:
                raise RuntimeError("Unexpected Error in function name:", function_name)

            if len(function_name) > 20 or len(function_name) == 0:
                raise RuntimeError("Function name must be greater than 0 and less than or equal to 20 characters")

            if not function_name.replace("_", "").isalnum():
                raise RuntimeError("Function name must contain only letters, numbers, or underscores")
