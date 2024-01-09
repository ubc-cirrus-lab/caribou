import os
import queue
from typing import Optional

from multi_x_serverless.deployment.client.cli.config_schema import Provider
from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.client.deploy.models.function import Function
from multi_x_serverless.deployment.client.deploy.models.function_instance import FunctionInstance
from multi_x_serverless.deployment.client.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.client.deploy.models.workflow import Workflow
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessFunction


class WorkflowBuilder:
    def build_workflow(self, config: Config) -> Workflow:  # pylint: disable=too-many-locals
        resources: list[Function] = []

        # A workflow consists of two parts:
        # 1. Set of functions (Resources) that are deployed to a serverless platform
        # 2. A DAG of the workflow that defines the order of execution of the function
        # instances (one function can be executed multiple times with different inputs and different predecessors)

        # Both of these are later used to build the DAG
        function_name_to_function: dict[str, MultiXServerlessFunction] = {}
        entry_point: Optional[MultiXServerlessFunction] = None

        # First, we create the functions (the resources that we deploy to the serverless platform)
        for function in config.workflow_app.functions.values():
            function_deployment_name = f"{config.workflow_name}-{function.name}"
            function_role = self.get_function_role(config, function_deployment_name)
            if function.regions_and_providers and "providers" in function.regions_and_providers:
                providers = (
                    function.regions_and_providers["providers"]
                    if function.regions_and_providers["providers"]
                    else config.regions_and_providers["providers"]
                )
            else:
                providers = config.regions_and_providers["providers"]
            self._verify_providers(providers)
            resources.append(
                Function(
                    name=function_deployment_name,
                    # TODO (#22): Add function specific environment variables
                    environment_variables=config.environment_variables,
                    runtime=config.python_version,
                    handler=function.handler,
                    role=function_role,
                    deployment_package=DeploymentPackage(),
                    home_regions=config.home_regions,
                    entry_point=function.entry_point,
                    providers=providers,
                )
            )
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
            name=f"{entry_point.name}:entry_point:{index_in_dag}",
            entry_point=entry_point.entry_point,
            regions_and_providers=entry_point.regions_and_providers
            if entry_point.regions_and_providers
            else config.regions_and_providers,
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
                f"{multi_x_serverless_function.name}:{predecessor_instance_name_for_instance}_{predecessor_index}_{successor_of_predecessor_index}:{index_in_dag}"  # pylint: disable=line-too-long
                if not multi_x_serverless_function.is_waiting_for_predecessors()
                else f"{multi_x_serverless_function.name}:merge:{index_in_dag}"
            )

            index_in_dag += 1
            # If the function is waiting for its predecessors, there can only be one instance of the function
            # Otherwise, we create a new instance of the function for every predecessor
            if function_instance_name not in function_instances:
                function_instances[function_instance_name] = FunctionInstance(
                    name=function_instance_name,
                    entry_point=multi_x_serverless_function.entry_point,
                    regions_and_providers=multi_x_serverless_function.regions_and_providers
                    if multi_x_serverless_function.regions_and_providers
                    else config.regions_and_providers,
                    function_resource_name=multi_x_serverless_function.name,
                )
                for successor_of_predecessor_i, successor in enumerate(
                    config.workflow_app.get_successors(multi_x_serverless_function)
                ):
                    functions_to_visit.put((successor.handler, function_instance_name, successor_of_predecessor_i))

            edges.append((predecessor_instance_name, function_instance_name))

        functions: list[FunctionInstance] = list(function_instances.values())
        return Workflow(resources=resources, functions=functions, edges=edges, name=config.workflow_name, config=config)

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

    def _verify_providers(self, providers: list[dict]) -> None:
        for provider in providers:
            Provider(**provider)

    def get_function_role(self, config: Config, function_name: str) -> IAMRole:
        role_name = f"{function_name}-role"

        if config.iam_policy_file:
            filename = os.path.join(config.project_dir, ".multi-x-serverless", config.iam_policy_file)
        else:
            filename = os.path.join(config.project_dir, ".multi-x-serverless", "iam_policy.yml")

        return IAMRole(role_name=role_name, policy=filename)
