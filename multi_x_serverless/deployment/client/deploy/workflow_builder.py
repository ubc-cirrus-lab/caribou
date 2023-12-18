from multi_x_serverless.deployment.client.deploy.models import (
    Workflow,
    Function,
    IAMRole,
    DeploymentPackage,
    FunctionInstance,
)
from multi_x_serverless.deployment.client.wrapper import MultiXServerlessFunction
from multi_x_serverless.deployment.client.config import Config

import os
import queue
import uuid


class WorkflowBuilder(object):
    def build_workflow(self, config: Config) -> Workflow:
        resources: list[Function] = []

        # A workflow consists of two parts:
        # 1. Set of functions (Resources) that are deployed to a serverless platform
        # 2. A DAG of the workflow that defines the order of execution of the function instances (one function can be executed multiple times with different inputs and different predecessors)

        # Both of these are later used to build the DAG
        function_name_to_function: dict[str, MultiXServerlessFunction] = {}
        entry_point: MultiXServerlessFunction = None

        # First, we create the functions (the resources that we deploy to the serverless platform)
        for function in config.workflow_app.functions:
            function_deployment_name = f"{config.workflow_name}-{function.name}"
            function_role = self.get_function_role(config, function_deployment_name)
            resources.append(
                Function(
                    name=function_deployment_name,
                    environment_variables=config.environment_variables,
                    runtime=config.python_version,
                    handler=function.handler,
                    timeout=function.timeout,
                    memory=function.memory,
                    role=function_role,
                    deployment_package=DeploymentPackage(),
                    region_group=function.region_group,
                )
            )
            function.deplyoment_name = function_deployment_name
            function_name_to_function[function.name] = function
            if function.entry_point and not entry_point:
                entry_point = function
            elif function.entry_point and entry_point:
                raise RuntimeError("Multiple entry points defined")

        if not entry_point:
            raise RuntimeError("No entry point defined")

        # Now we build the DAG
        # function_instances maps function instance names to function instances.
        # If a function has multiple predecessors (is waiting for predecessor data) then there 
        # will only be one instance of the function.
        # Otherwise, there will be one instance of the function for every function that calls it.
        function_instances: dict[str, FunctionInstance] = {}
        # edges is a list of tuples that represent the edges in the DAG (mapping instance names to instance names)
        edges: list[tuple[str, str]] = []

        # We use a queue to visit all functions in the DAG in a breadth-first manner
        functions_to_visit = queue.Queue()

        # We start with the entry point
        predecessor_instance = FunctionInstance(
            name=f"{entry_point.name}-{uuid.uuid4()}",
            resource_name=entry_point.deployment_name,
            entry_point=entry_point.entry_point,
            timeout=entry_point.timeout,
            memory=entry_point.memory,
            region_group=entry_point.region_group,
        )
        function_instances[predecessor_instance.name] = predecessor_instance

        for successor in entry_point.get_successors(config.workflow_app):
            functions_to_visit.put((successor.name, predecessor_instance.name))

        while not functions_to_visit.empty():
            function_to_visit, predecessor_instance_name: tuple[str, str] = functions_to_visit.get()
            function: MultiXServerlessFunction = function_name_to_function[function_to_visit]
            function_instance_name = (
                f"{function.name}-{uuid.uuid4()}" if not function.is_waiting_for_predecessors() else function.name
            )
            # If the function is waiting for its predecessors, there can only be one instance of the function
            # Otherwise, we create a new instance of the function for every predecessor
            if function_instance_name not in function_instances:
                function_instances[function_instance_name] = FunctionInstance(
                    name=function_instance_name,
                    resource_name=function.deployment_name,
                    entry_point=function.entry_point,
                    timeout=function.timeout,
                    memory=function.memory,
                    region_group=function.region_group,
                )
                for successor in function.get_successors(config.workflow_app):
                    functions_to_visit.put(successor.name)
            
            edges.append((predecessor_instance_name, function_instance_name))

        return Workflow(resources=resources.values(), functions=function_instances.values(), edges=edges)

    def get_function_role(self, config: Config, function_name: str) -> IAMRole:
        role_name = f"{function_name}-role"

        if config.iam_policy_file:
            filename = os.path.join(config.project_dir, ".multi-x-serverless", config.iam_policy_file)
        else:
            filename = os.path.join(config.project_dir, ".multi-x-serverless", "iam_policy.yml")

        return IAMRole(name=role_name, policy_file=filename)
