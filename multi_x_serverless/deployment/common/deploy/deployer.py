import json
from typing import Optional

import botocore.exceptions

from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    SOLVER_UPDATE_CHECKER_RESOURCE_TABLE,
)
from multi_x_serverless.deployment.common.deploy.deployment_packager import DeploymentPackager
from multi_x_serverless.deployment.common.deploy.executor import Executor
from multi_x_serverless.deployment.common.deploy.models.deployment_plan import DeploymentPlan
from multi_x_serverless.deployment.common.deploy.models.endpoints import Endpoints
from multi_x_serverless.deployment.common.deploy.models.workflow import Workflow
from multi_x_serverless.deployment.common.deploy.workflow_builder import WorkflowBuilder


class Deployer:
    def __init__(
        self,
        config: Config,
        workflow_builder: WorkflowBuilder,
        deployment_packager: DeploymentPackager,
        executor: Optional[Executor],
    ) -> None:
        self._config = config
        self._workflow_builder = workflow_builder
        self._deployment_packager = deployment_packager
        self._executor = executor
        self._endpoints = Endpoints()

    def re_deploy(
        self,
        function_to_deployment_regions: dict[str, list[dict[str, str]]],
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, list[dict[str, str]]],
    ) -> None:
        if not isinstance(function_to_deployment_regions, dict):
            raise TypeError("function_to_deployment_regions must be a dictionary")
        if not isinstance(workflow_function_descriptions, list):
            raise TypeError("workflow_function_descriptions must be a list")
        if not isinstance(deployed_regions, dict):
            raise TypeError("deployed_regions must be a dictionary")
        try:
            self._re_deploy(function_to_deployment_regions, workflow_function_descriptions, deployed_regions)
        except botocore.exceptions.ClientError as e:
            raise DeploymentError(e) from e

    def _re_deploy(
        self,
        function_to_deployment_regions: dict[str, list[dict[str, str]]],
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, list[dict[str, str]]],
    ) -> None:
        if self._config.workflow_id is None or self._config.workflow_id == "{}":
            raise RuntimeError("Workflow id is not set correctly")

        filtered_function_to_deployment_regions = self._filter_function_to_deployment_regions(
            function_to_deployment_regions, deployed_regions
        )

        workflow = self._workflow_builder.re_build_workflow(
            self._config, filtered_function_to_deployment_regions, workflow_function_descriptions
        )

        self._deployment_packager.re_build(workflow, self._endpoints.get_deployment_manager_client())

        deployment_plan = DeploymentPlan(workflow.get_deployment_instructions())

        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        self._executor.execute(deployment_plan)

        merged_deployer_regions = self._merge_deployed_regions(
            deployed_regions, filtered_function_to_deployment_regions
        )

        self._update_workflow_to_deployer_server(workflow, self._config.workflow_id, merged_deployer_regions)

    def _filter_function_to_deployment_regions(
        self,
        function_to_deployment_regions: dict[str, list[dict[str, str]]],
        deployed_regions: dict[str, list[dict[str, str]]],
    ) -> dict[str, list[dict[str, str]]]:
        filtered_function_to_deployment_regions = {}
        for function_name, deployment_regions in function_to_deployment_regions.items():
            if function_name in deployed_regions:
                filtered_function_to_deployment_regions[function_name] = [
                    region for region in deployment_regions if region not in deployed_regions[function_name]
                ]
            else:
                filtered_function_to_deployment_regions[function_name] = deployment_regions
        return filtered_function_to_deployment_regions

    def _merge_deployed_regions(
        self,
        deployed_regions: dict[str, list[dict[str, str]]],
        filtered_function_to_deployment_regions: dict[str, list[dict[str, str]]],
    ) -> dict[str, list[dict[str, str]]]:
        merged_deployed_regions = deployed_regions
        for function_name, deployment_regions in filtered_function_to_deployment_regions.items():
            if function_name in merged_deployed_regions:
                merged_deployed_regions[function_name].extend(deployment_regions)
            else:
                merged_deployed_regions[function_name] = deployment_regions
        return merged_deployed_regions

    def deploy(self, regions: list[dict[str, str]]) -> None:
        try:
            self._deploy(regions)
        except botocore.exceptions.ClientError as e:
            if type(e).__name__ == "ResourceNotFoundException":
                raise DeploymentError("Are all the resources at the server side created?") from e
            raise DeploymentError(e) from e

    def _deploy(self, regions: list[dict[str, str]]) -> None:
        # Build the workflow (DAG of the workflow)
        workflow = self._workflow_builder.build_workflow(self._config, regions)

        self._set_workflow_id(workflow)

        # Upload the workflow to the solver
        self._upload_workflow_to_solver_update_checker(workflow, self._config.workflow_id)

        # Build the workflow resources, e.g. deployment packages, iam roles, etc.
        self._deployment_packager.build(self._config, workflow)

        # Chain the commands needed to deploy all the built resources to the serverless platform
        deployment_plan = DeploymentPlan(workflow.get_deployment_instructions())

        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        # Execute the deployment plan
        self._executor.execute(deployment_plan)

        self._upload_workflow_to_deployer_server(workflow, self._config.workflow_id)
        self._upload_deployment_package_resource(workflow)

    def _set_workflow_id(self, workflow: Workflow) -> None:
        workflow_id = f"{workflow.name}-{workflow.version}"
        self._config.set_workflow_id(workflow_id)

    def _upload_workflow_to_solver_update_checker(self, workflow: Workflow, workflow_id: str) -> None:
        workflow_config = workflow.get_instance_description().to_json()

        payload = {
            "workflow_id": workflow_id,
            "workflow_config": workflow_config,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_solver_update_checker_client().set_value_in_table(
            SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, workflow_id, payload_json
        )

    def _upload_workflow_to_deployer_server(self, workflow: Workflow, workflow_id: str) -> None:
        workflow_function_descriptions = workflow.get_function_description()
        workflow_function_descriptions_json = json.dumps(workflow_function_descriptions)
        deployment_config_json = self._config.to_json()
        deployed_regions = workflow.get_deployed_regions_initial_deployment()
        deployed_regions_json = json.dumps(deployed_regions)

        payload = {
            "workflow_id": workflow_id,
            "workflow_function_descriptions": workflow_function_descriptions_json,
            "deployment_config": deployment_config_json,
            "deployed_regions": deployed_regions_json,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, workflow_id, payload_json
        )

    def _update_workflow_to_deployer_server(
        self, workflow: Workflow, workflow_id: str, deployed_regions: dict[str, list[dict[str, str]]]
    ) -> None:
        workflow_function_descriptions = workflow.get_function_description()
        workflow_function_descriptions_json = json.dumps(workflow_function_descriptions)
        deployment_config_json = self._config.to_json()
        deployed_regions_json = json.dumps(deployed_regions)

        payload = {
            "workflow_id": workflow_id,
            "workflow_function_descriptions": workflow_function_descriptions_json,
            "deployment_config": deployment_config_json,
            "deployed_regions": deployed_regions_json,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, workflow_id, payload_json
        )

    def _upload_deployment_package_resource(self, workflow: Workflow) -> None:
        deployment_packege_filename = workflow.get_deployment_packages()[0].filename

        if deployment_packege_filename is None:
            raise RuntimeError("Deployment package filename is None")

        with open(deployment_packege_filename, "rb") as deployment_package_file:
            deployment_package = deployment_package_file.read()

        self._endpoints.get_deployment_manager_client().upload_resource(
            f"deployment_package_{self._config.workflow_id}", deployment_package
        )


def create_default_deployer(config: Config) -> Deployer:
    return Deployer(
        config,
        WorkflowBuilder(),
        DeploymentPackager(config),
        Executor(config),
    )


def create_deletion_deployer(config: Config) -> Deployer:
    return Deployer(config, WorkflowBuilder(), DeploymentPackager(config), None)


class DeploymentError(Exception):
    pass
