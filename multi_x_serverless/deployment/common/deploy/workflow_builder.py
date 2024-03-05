import logging
import os
import queue
from collections import defaultdict
from typing import Any, Optional

from multi_x_serverless.common.provider import Provider as ProviderEnum
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessFunction
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.config.config_schema import Provider
from multi_x_serverless.deployment.common.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.common.deploy.models.function import Function
from multi_x_serverless.deployment.common.deploy.models.function_instance import FunctionInstance
from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.workflow import Workflow

logger = logging.getLogger(__name__)


class WorkflowBuilder:
    def build_workflow(  # pylint: disable=too-many-branches
        self, config: Config, regions: list[dict[str, str]]
    ) -> Workflow:
        # A workflow consists of two parts:
        # 1. Set of functions (Resources) that are deployed to a serverless platform
        # 2. A DAG of the workflow that defines the order of execution of the function
        # instances (one function can be executed multiple times with different inputs and different predecessors)

        if len(regions) == 0:
            raise RuntimeError("At least one region must be defined")

        home_regions_resources: list[Function] = []

        # Both of these are later used to build the DAG
        function_name_to_function: dict[str, MultiXServerlessFunction] = {}
        entry_point: Optional[MultiXServerlessFunction] = None

        for region in regions:
            if config.workflow_name != config.workflow_app.name:
                raise RuntimeError("Workflow name in config and workflow app must match")

            if config.workflow_version != config.workflow_app.version:
                raise RuntimeError("Workflow version in config and workflow app must match")

            # First, we create the functions (the resources that we deploy to the serverless platform)
            for function in config.workflow_app.functions.values():
                function_deployment_name = self._get_function_name(config, function, region)
                function_role = self.get_function_role(config, function_deployment_name)
                if function.regions_and_providers and "providers" in function.regions_and_providers:
                    providers = (
                        function.regions_and_providers["providers"]
                        if function.regions_and_providers["providers"]
                        else config.regions_and_providers["providers"]
                    )
                else:
                    providers = config.regions_and_providers["providers"]
                home_region_providers = [provider_region["provider"] for provider_region in config.home_regions]
                for provider in home_region_providers:
                    if provider not in providers:
                        raise RuntimeError(
                            f"Home region provider {provider} is not defined in providers for function {function.name}"
                        )
                self._verify_providers(providers)

                merged_env_vars = self.merge_environment_variables(
                    function.environment_variables, config.environment_variables
                )
                home_regions_resources.append(
                    Function(
                        name=function_deployment_name,
                        environment_variables=merged_env_vars,
                        runtime=config.python_version,
                        handler=function.handler,
                        role=function_role,
                        deployment_package=DeploymentPackage(),
                        deploy_region=region,
                        entry_point=function.entry_point,
                        providers=providers,
                    )
                )
                function.name = function_deployment_name
                function_name_to_function[function.handler] = function
                if function.entry_point and not entry_point:
                    entry_point = function
                elif function.entry_point and entry_point:
                    raise RuntimeError("Multiple entry points defined")

        if not entry_point:
            raise RuntimeError("No entry point defined")

        # Now we build the DAG
        # function_instances maps function instance names to function instances.
        # If a function has multiple predecessors (is waiting for predecessor data) then there
        # will only be one instance of the function.
        # Otherwise, there will be one instance of the function for every function that calls it.
        function_instances: dict[str, FunctionInstance] = {}
        # edges is a list of tuples that represent the edges in the DAG (mapping instance names to instance names)
        edges: list[tuple[str, str]] = []

        # We use a queue to visit all functions in the DAG in a breadth-first manner
        functions_to_visit: queue.Queue = queue.Queue()

        index_in_dag = 0
        # We start with the entry point
        predecessor_instance = FunctionInstance(
            name=f"{self._get_function_name_without_provider_and_region(entry_point.name)}:entry_point:{index_in_dag}",
            entry_point=entry_point.entry_point,
            regions_and_providers=self._merge_and_verify_regions_and_providers(
                entry_point.regions_and_providers, config
            ),
            function_resource_name=entry_point.name,
        )
        index_in_dag += 1
        function_instances[predecessor_instance.name] = predecessor_instance

        self._cycle_check(entry_point, config)

        for successor_of_current_index, successor in enumerate(config.workflow_app.get_successors(entry_point)):
            functions_to_visit.put((successor.handler, predecessor_instance.name, successor_of_current_index))

        while not functions_to_visit.empty():
            function_to_visit, predecessor_instance_name, successor_of_predecessor_index = functions_to_visit.get()
            multi_x_serverless_function: MultiXServerlessFunction = function_name_to_function[function_to_visit]
            predecessor_instance_name_for_instance = predecessor_instance_name.split(":", maxsplit=1)[0]
            predecessor_index = predecessor_instance_name.split(":")[-1]
            function_instance_name = (
                f"{self._get_function_name_without_provider_and_region(multi_x_serverless_function.name)}:{predecessor_instance_name_for_instance}_{predecessor_index}_{successor_of_predecessor_index}:{index_in_dag}"  # pylint: disable=line-too-long
                if not multi_x_serverless_function.is_waiting_for_predecessors()
                else f"{self._get_function_name_without_provider_and_region(multi_x_serverless_function.name)}:sync:"  # pylint: disable=line-too-long
            )

            index_in_dag += 1
            # If the function is waiting for its predecessors, there can only be one instance of the function
            # Otherwise, we create a new instance of the function for every predecessor
            if function_instance_name not in function_instances:
                function_instances[function_instance_name] = FunctionInstance(
                    name=function_instance_name,
                    entry_point=multi_x_serverless_function.entry_point,
                    regions_and_providers=self._merge_and_verify_regions_and_providers(
                        multi_x_serverless_function.regions_and_providers, config
                    ),
                    function_resource_name=multi_x_serverless_function.name,
                )
                for successor_of_predecessor_i, successor in enumerate(
                    config.workflow_app.get_successors(multi_x_serverless_function)
                ):
                    functions_to_visit.put((successor.handler, function_instance_name, successor_of_predecessor_i))

            edges.append((predecessor_instance_name, function_instance_name))

        functions: list[FunctionInstance] = list(function_instances.values())
        return Workflow(
            resources=home_regions_resources,
            functions=functions,
            edges=edges,
            name=config.workflow_name,
            config=config,
            version=config.workflow_version,
        )

    def _merge_and_verify_regions_and_providers(  # pylint: disable=too-many-branches
        self, regions_and_providers: Optional[dict[str, Any]], config: Config
    ) -> dict[str, Any]:
        result_regions_and_providers = config.regions_and_providers.copy()
        if regions_and_providers:
            if "providers" in regions_and_providers:
                result_regions_and_providers["providers"] = regions_and_providers["providers"]
            possible_providers = [provider.value for provider in ProviderEnum]
            defined_providers = [
                provider_name
                for provider_name in result_regions_and_providers["providers"].keys()
                if provider_name in possible_providers
            ]
            allowed_regions_collection = set()
            if "allowed_regions" in regions_and_providers:
                result_regions_and_providers["allowed_regions"] = regions_and_providers["allowed_regions"]
                allowed_regions = regions_and_providers["allowed_regions"]
                if not allowed_regions:
                    allowed_regions = []
                if allowed_regions and not isinstance(allowed_regions, list):
                    raise RuntimeError("allowed_regions must be a list")
                for provider_region in allowed_regions:
                    if "provider" not in provider_region or "region" not in provider_region:
                        raise RuntimeError(f"Region {provider_region} must have both provider and region defined")
                    allowed_regions_collection.add(f"{provider_region['provider']}-{provider_region['region']}")
                    if not isinstance(provider_region, dict):
                        raise RuntimeError("allowed_regions must be a list of strings")
                    provider = provider_region["provider"]
                    if provider not in [provider.value for provider in ProviderEnum]:
                        raise RuntimeError(f"Provider {provider} is not supported")
                    if provider not in defined_providers:
                        raise RuntimeError(f"Provider {provider} is not defined in providers")
            if "disallowed_regions" in regions_and_providers:
                result_regions_and_providers["disallowed_regions"] = regions_and_providers["disallowed_regions"]
                disallowed_regions = regions_and_providers["disallowed_regions"]
                if not disallowed_regions:
                    disallowed_regions = []
                if disallowed_regions and not isinstance(disallowed_regions, list):
                    raise RuntimeError("disallowed_regions must be a list")
                for provider_region in disallowed_regions:
                    if "provider" not in provider_region or "region" not in provider_region:
                        raise RuntimeError(f"Region {provider_region} must have both provider and region defined")
                    if f"{provider_region['provider']}-{provider_region['region']}" in allowed_regions_collection:
                        raise RuntimeError(f"Region {provider_region} cannot be both allowed and disallowed")
                    if not isinstance(provider_region, dict):
                        raise RuntimeError("disallowed_regions must be a list of strings")
                    provider = provider_region["provider"]
                    if provider not in [provider.value for provider in ProviderEnum]:
                        raise RuntimeError(f"Provider {provider} is not supported")
                    if provider not in defined_providers:
                        raise RuntimeError(f"Provider {provider} is not defined in providers")
                    if provider_region in config.home_regions:
                        raise RuntimeError(f"Region {provider_region} cannot be both home and disallowed")
        return result_regions_and_providers

    def _get_function_name(self, config: Config, function: MultiXServerlessFunction, region: dict[str, str]) -> str:
        # A function name is of the form <workflow_name>-<workflow_version>-<function_name>_<provider>-<region>
        # This is used to uniquely identify a function with respect to a workflow,
        # its version, the provider and the region
        name = (
            f"{config.workflow_name}-{config.workflow_version}-{function.name}_{region['provider']}-{region['region']}"
        )
        return name.replace(".", "_")

    def _get_function_name_without_provider_and_region(self, function_name: str) -> str:
        # A function name is of the form <workflow_name>-<workflow_version>-<function_name>_<provider>-<region>
        # We want to return <workflow_name>-<workflow_version>-<function_name>
        # <workflow_name>-<workflow_version>-<function_name> can contain _
        return "_".join(function_name.split("_")[:-1])

    def re_build_workflow(
        self,
        config: Config,
        function_to_deployment_region: dict[str, dict[str, str]],
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, dict[str, Any]],
    ) -> Workflow:
        resources: list[Function] = []

        function_name_to_description_to_update_functions = defaultdict(list)

        for function_name, deployment_region in function_to_deployment_region.items():
            if function_name in deployed_regions:
                continue
            key = self._get_function_name_without_provider_and_region(function_name)
            value = (function_name, deployment_region)
            function_name_to_description_to_update_functions[key].append(value)

        for function in workflow_function_descriptions:
            function_name_without_provider_and_region = self._get_function_name_without_provider_and_region(
                function["name"]
            )

            if function_name_without_provider_and_region in function_name_to_description_to_update_functions:
                for function_name, deployment_region in function_name_to_description_to_update_functions[
                    function_name_without_provider_and_region
                ]:
                    # This is a function that was already deployed and we are adding a new region to it
                    resources.append(
                        Function(
                            name=function_name,
                            environment_variables=function["environment_variables"],
                            runtime=function["runtime"],
                            handler=function["handler"],
                            role=IAMRole(function["role"]["policy_file"], f"{function_name}-role"),
                            deployment_package=DeploymentPackage(),
                            deploy_region=deployment_region,
                            entry_point=function["entry_point"],
                            providers=function["providers"],
                        )
                    )
            else:
                resources.append(
                    Function(
                        name=function["name"],
                        environment_variables=function["environment_variables"],
                        runtime=function["runtime"],
                        handler=function["handler"],
                        role=IAMRole(function["role"]["policy_file"], function["role"]["role_name"]),
                        deployment_package=DeploymentPackage(),
                        deploy_region=deployed_regions[function["name"]]["deploy_region"],
                        entry_point=function["entry_point"],
                        providers=function["providers"],
                        deploy=False,
                    )
                )

        return Workflow(
            resources=resources,
            functions=[],
            edges=[],
            name=config.workflow_name,
            config=config,
            version=config.workflow_version,
        )

    def _cycle_check(self, function: MultiXServerlessFunction, config: Config) -> None:
        visiting: set[MultiXServerlessFunction] = set()
        visited: set[MultiXServerlessFunction] = set()
        self._dfs(function, visiting, visited, config)

    def _dfs(
        self,
        node: MultiXServerlessFunction,
        visiting: set[MultiXServerlessFunction],
        visited: set[MultiXServerlessFunction],
        config: Config,
    ) -> None:
        visiting.add(node)
        for successor in config.workflow_app.get_successors(node):
            if successor in visiting:
                raise RuntimeError(f"Cycle detected: {successor.name} is being visited again")
            if successor not in visited:
                self._dfs(successor, visiting, visited, config)
        visiting.remove(node)
        visited.add(node)

    def _verify_providers(self, providers: dict[str, Any]) -> None:
        for provider in providers.values():
            Provider(**provider)

    def get_function_role(self, config: Config, function_name: str) -> IAMRole:
        if config.project_dir is None:
            raise RuntimeError("project_dir must be defined")
        role_name = f"{function_name}-role"

        if config.iam_policy_file:
            filename = os.path.join(config.project_dir, ".multi-x-serverless", config.iam_policy_file)
        else:
            filename = os.path.join(config.project_dir, ".multi-x-serverless", "iam_policy.yml")

        return IAMRole(role_name=role_name, policy_file=filename)

    def merge_environment_variables(
        self, function_env_vars: Optional[list[dict[str, str]]], config_env_vars: dict[str, str]
    ) -> dict[str, str]:
        if not function_env_vars:
            return config_env_vars

        merged_env_vars: dict[str, str] = dict(config_env_vars)
        # overwrite config env vars with function env vars if duplicate
        for env_var in function_env_vars:
            merged_env_vars[env_var["key"]] = env_var["value"]

        return merged_env_vars
