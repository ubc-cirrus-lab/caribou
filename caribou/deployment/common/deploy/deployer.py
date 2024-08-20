import json
import logging
from typing import Any, Optional

import botocore.exceptions

from caribou.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    DEPLOYMENT_RESOURCES_TABLE,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
)
from caribou.common.models.endpoints import Endpoints
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployment_packager import DeploymentPackager
from caribou.deployment.common.deploy.executor import Executor
from caribou.deployment.common.deploy.models.deployment_plan import DeploymentPlan
from caribou.deployment.common.deploy.models.workflow import Workflow
from caribou.deployment.common.deploy.workflow_builder import WorkflowBuilder

logger = logging.getLogger()


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
        self._workflow: Optional[Workflow] = None

    def deploy(self, regions: list[dict[str, str]]) -> None:
        try:
            self._deploy(regions)
        except botocore.exceptions.ClientError as e:
            if type(e).__name__ == "ResourceNotFoundException":
                raise DeploymentError("Are all the resources at the server side created?") from e
            raise DeploymentError(e) from e

    def _deploy(self, regions: list[dict[str, str]]) -> None:
        logger.info("Deploying workflow %s with version %s", self._config.workflow_name, self._config.workflow_version)
        # Build the workflow (DAG of the workflow)
        self._workflow = self._workflow_builder.build_workflow(self._config, regions)

        self._set_workflow_id()

        already_deployed = self._get_workflow_already_deployed()
        if already_deployed:
            raise DeploymentError(
                f"Workflow {self._config.workflow_name} with version {self._config.workflow_version} already deployed, please use a different version number"  # pylint: disable=line-too-long
            )

        # Check for name and version restrictions (e.g. no spaces, no certain special characters etc.)
        self._workflow.verify_name_and_version()

        # Upload the workflow to the solver
        self._upload_workflow_to_deployment_manager()

        # Build the workflow resources, e.g. deployment packages, iam roles, etc.
        logger.info("Building deployment package")

        self._deployment_packager.build(self._config, self._workflow)

        # Chain the commands needed to deploy all the built resources to the serverless platform
        logger.info("Building deployment plan")

        # Redeployment does not require a deployment package (Refer to issue #293)
        allow_no_deployment_package: bool = False
        self._workflow.allow_no_deployment_package = allow_no_deployment_package
        deployment_plan = DeploymentPlan(self._workflow.get_deployment_instructions())

        assert self._executor is not None, "Executor is None, this should not happen"

        # Execute the deployment plan
        logger.info("Executing deployment plan")
        self._executor.execute(deployment_plan)

        # Upload the workflow to the deployer server
        logger.info("Uploading workflow to configuration server")
        self._upload_workflow_to_deployer_server()

        # Disabled as part of issue #293
        # self._upload_deployment_package_resource()

        self._upload_workflow_placement_decision()

        logger.info("Workflow %s with version %s deployed", self._config.workflow_name, self._config.workflow_version)
        logger.info("Workflow id: %s", self._config.workflow_id)

    def re_deploy(
        self,
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, dict[str, str]],
        specific_staging_area_data: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        if not isinstance(workflow_function_descriptions, list):
            raise TypeError("workflow_function_descriptions must be a list")
        if not isinstance(deployed_regions, dict):
            raise TypeError("deployed_regions must be a dictionary")
        try:
            return self._re_deploy(workflow_function_descriptions, deployed_regions, specific_staging_area_data)
        except botocore.exceptions.ClientError as e:
            raise DeploymentError(e) from e

    def _re_deploy(
        self,
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, dict[str, str]],
        specific_staging_area_data: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        function_to_deployment_regions = self._get_function_to_deployment_regions(specific_staging_area_data)

        filtered_function_to_deployment_regions = self._filter_function_to_deployment_regions(
            function_to_deployment_regions, deployed_regions
        )

        self._workflow = self._workflow_builder.re_build_workflow(
            self._config, filtered_function_to_deployment_regions, workflow_function_descriptions, deployed_regions
        )

        # Disabled as part of issue #293
        # self._deployment_packager.re_build(self._workflow, self._endpoints.get_deployment_resources_client())

        # Redeployment does not require a deployment package (Refer to issue #293)
        allow_no_deployment_package: bool = True
        self._workflow.allow_no_deployment_package = allow_no_deployment_package
        deployment_plan = DeploymentPlan(self._workflow.get_deployment_instructions())

        assert self._executor is not None, "Executor is None, this should not happen"

        self._executor.execute(deployment_plan)

        self._update_deployed_regions(deployed_regions)
        return self._get_new_deployment_instances(specific_staging_area_data)

    def _get_function_to_deployment_regions(self, staging_area_data: dict) -> dict[str, dict[str, str]]:
        function_to_deployment_regions: dict[str, dict[str, str]] = {}
        for instance_name, placement in staging_area_data.items():
            function_name = (
                instance_name.split(":", maxsplit=1)[0]
                + "_"
                + placement["provider_region"]["provider"]
                + "-"
                + placement["provider_region"]["region"]
            )
            if function_name not in function_to_deployment_regions:
                function_to_deployment_regions[function_name] = {
                    "provider": placement["provider_region"]["provider"],
                    "region": placement["provider_region"]["region"],
                }
        return function_to_deployment_regions

    def _filter_function_to_deployment_regions(
        self,
        function_to_deployment_regions: dict[str, dict[str, str]],
        deployed_regions: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        filtered_function_to_deployment_regions: dict[str, dict[str, str]] = {}
        for function_name, deployment_regions in function_to_deployment_regions.items():
            if function_name not in deployed_regions:
                filtered_function_to_deployment_regions[function_name] = deployment_regions
        return filtered_function_to_deployment_regions

    def _set_workflow_id(self) -> None:
        assert self._workflow is not None, "Workflow is None, this should not happen"
        workflow_id = f"{self._workflow.name}-{self._workflow.version}"
        self._config.set_workflow_id(workflow_id)

    def _upload_workflow_to_deployment_manager(self) -> None:
        assert self._workflow is not None, "Workflow is None, this should not happen"
        workflow_config = self._workflow.get_workflow_config().to_json()

        payload = {
            "workflow_id": self._config.workflow_id,
            "workflow_config": workflow_config,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._config.workflow_id, payload_json
        )

    def _get_workflow_already_deployed(self) -> bool:
        return self._endpoints.get_deployment_manager_client().get_key_present_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._config.workflow_id
        )

    def _get_new_deployment_instances(self, staging_area_data: dict) -> dict[str, dict[str, Any]]:
        assert self._workflow is not None, "Workflow is None, this should not happen"

        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        deployment_instances = self._workflow.get_deployment_instances(staging_area_data)
        return deployment_instances

    def _upload_workflow_placement_decision(self) -> None:
        assert self._workflow is not None, "Workflow is None, this should not happen"
        assert self._executor is not None, "Executor is None, this should not happen"

        workflow_placement_decision = self._workflow.get_workflow_placement_decision_initial_deployment()
        workflow_placement_decision_json = json.dumps(workflow_placement_decision)

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._config.workflow_id, workflow_placement_decision_json
        )

    def _upload_workflow_to_deployer_server(self) -> None:
        assert self._workflow is not None, "Workflow is None, this should not happen"
        assert self._executor is not None, "Executor is None, this should not happen"
        workflow_function_descriptions = self._workflow.get_function_description()
        workflow_function_descriptions_json = json.dumps(workflow_function_descriptions)
        deployment_config_json = self._config.to_json()
        deployed_regions = self._workflow.get_deployed_regions_initial_deployment(self._executor.resource_values)
        deployed_regions_json = json.dumps(deployed_regions)

        payload = {
            "workflow_id": self._config.workflow_id,
            "workflow_function_descriptions": workflow_function_descriptions_json,
            "deployment_config": deployment_config_json,
            "deployed_regions": deployed_regions_json,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_deployment_resources_client().set_value_in_table(
            DEPLOYMENT_RESOURCES_TABLE, self._config.workflow_id, payload_json
        )

        self._workflow.set_deployed_regions(deployed_regions)

    def _update_deployed_regions(self, deployed_regions: dict[str, dict[str, str]]) -> None:
        assert self._workflow is not None, "Workflow is None, this should not happen"
        assert self._executor is not None, "Executor is None, this should not happen"
        self._workflow.update_deployed_regions(self._executor.resource_values, deployed_regions)

    def _upload_deployment_package_resource(self) -> None:
        assert self._workflow is not None, "Workflow is None, this should not happen"
        deployment_packege_filename = self._workflow.get_deployment_packages()[0].filename

        if deployment_packege_filename is None:
            raise RuntimeError("Deployment package filename is None")

        # Append zip extension if not present
        if not deployment_packege_filename.endswith(".zip"):
            deployment_packege_filename = f"{deployment_packege_filename}.zip"

        if deployment_packege_filename is None:
            raise RuntimeError("Deployment package filename is None")

        with open(deployment_packege_filename, "rb") as deployment_package_file:
            deployment_package = deployment_package_file.read()

        self._endpoints.get_deployment_resources_client().upload_resource(
            f"deployment_package_{self._config.workflow_id}", deployment_package
        )


def create_default_deployer(config: Config) -> Deployer:
    return Deployer(
        config,
        WorkflowBuilder(),
        DeploymentPackager(config),
        Executor(config),
    )


class DeploymentError(Exception):
    pass
