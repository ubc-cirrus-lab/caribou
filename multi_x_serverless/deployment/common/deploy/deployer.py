import json
from typing import Optional

import botocore.exceptions

from multi_x_serverless.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    SOLVER_UPDATE_CHECKER_RESOURCE_TABLE,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
    WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE,
)
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.deployment_packager import DeploymentPackager
from multi_x_serverless.deployment.common.deploy.executor import Executor
from multi_x_serverless.deployment.common.deploy.models.deployment_plan import DeploymentPlan
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
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, dict[str, str]],
    ) -> None:
        if not isinstance(workflow_function_descriptions, list):
            raise TypeError("workflow_function_descriptions must be a list")
        if not isinstance(deployed_regions, dict):
            raise TypeError("deployed_regions must be a dictionary")
        try:
            self._re_deploy(workflow_function_descriptions, deployed_regions)
        except botocore.exceptions.ClientError as e:
            raise DeploymentError(e) from e

    def _re_deploy(
        self,
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, dict[str, str]],
    ) -> None:
        if self._config.workflow_id is None or self._config.workflow_id == "{}":
            raise RuntimeError("Workflow id is not set correctly")

        workflow_deployed = self._get_workflow_already_deployed()

        if not workflow_deployed:
            raise DeploymentError(
                f"Workflow {self._config.workflow_name} with version {self._config.workflow_version} not deployed, something went wrong"  # pylint: disable=line-too-long
            )

        staging_area_data = self._endpoints.get_solver_update_checker_client().get_value_from_table(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._config.workflow_id
        )

        previous_workflow_placement_decision = self._endpoints.get_solver_update_checker_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._config.workflow_id
        )

        if previous_workflow_placement_decision is None:
            raise RuntimeError("Current workflow placement decision is None")

        previous_workflow_placement_decision_json = json.loads(previous_workflow_placement_decision)

        if staging_area_data is None:
            raise RuntimeError("Staging area data is None")

        staging_area_data_json = json.loads(staging_area_data)

        if not isinstance(staging_area_data_json, dict):
            raise RuntimeError("Staging area data is not a dictionary")

        function_to_deployment_regions = self._get_function_to_deployment_regions(staging_area_data_json)

        filtered_function_to_deployment_regions = self._filter_function_to_deployment_regions(
            function_to_deployment_regions, deployed_regions
        )

        workflow = self._workflow_builder.re_build_workflow(
            self._config, filtered_function_to_deployment_regions, workflow_function_descriptions, deployed_regions
        )

        self._deployment_packager.re_build(workflow, self._endpoints.get_deployment_manager_client())

        deployment_plan = DeploymentPlan(workflow.get_deployment_instructions())

        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        self._executor.execute(deployment_plan)

        merged_deployer_regions = self._merge_deployed_regions(
            deployed_regions, filtered_function_to_deployment_regions
        )

        self._update_workflow_to_deployer_server(workflow, merged_deployer_regions)
        self._update_workflow_placement_decision(
            workflow, staging_area_data, previous_workflow_placement_decision_json["instances"]
        )

    def _get_function_to_deployment_regions(self, staging_area_data: dict) -> dict[str, dict[str, str]]:
        function_to_deployment_regions: dict[str, dict[str, str]] = {}
        for instance_name, placement in staging_area_data["workflow_placement"].items():
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

    def _merge_deployed_regions(
        self,
        deployed_regions: dict[str, dict[str, str]],
        filtered_function_to_deployment_regions: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        merged_deployer_regions = deployed_regions.copy()
        for function_name, deployment_regions in filtered_function_to_deployment_regions.items():
            merged_deployer_regions[function_name] = deployment_regions
        return merged_deployer_regions

    def deploy(self, regions: list[dict[str, str]]) -> None:
        try:
            self._deploy(regions)
        except botocore.exceptions.ClientError as e:
            if type(e).__name__ == "ResourceNotFoundException":
                raise DeploymentError("Are all the resources at the server side created?") from e
            raise DeploymentError(e) from e

    def _deploy(self, regions: list[dict[str, str]]) -> None:
        print(f"Deploying workflow {self._config.workflow_name} with version {self._config.workflow_version}")
        # Build the workflow (DAG of the workflow)
        workflow = self._workflow_builder.build_workflow(self._config, regions)

        self._set_workflow_id(workflow)

        already_deployed = self._get_workflow_already_deployed()
        if already_deployed:
            raise DeploymentError(
                f"Workflow {self._config.workflow_name} with version {self._config.workflow_version} already deployed, please use a different version number"  # pylint: disable=line-too-long
            )

        # Upload the workflow to the solver
        self._upload_workflow_to_solver_update_checker(workflow)

        # Build the workflow resources, e.g. deployment packages, iam roles, etc.
        print("Building deployment package")
        self._deployment_packager.build(self._config, workflow)

        # Chain the commands needed to deploy all the built resources to the serverless platform
        print("Building deployment plan")
        deployment_plan = DeploymentPlan(workflow.get_deployment_instructions())

        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        # Execute the deployment plan
        print("Executing deployment plan")
        self._executor.execute(deployment_plan)

        # Upload the workflow to the deployer server
        print("Uploading workflow to configuration server")
        self._upload_workflow_to_deployer_server(workflow)
        self._upload_deployment_package_resource(workflow)
        self._upload_workflow_placement_decision(workflow)

        print(f"Workflow {self._config.workflow_name} with version {self._config.workflow_version} deployed")
        print(f"Workflow id: {self._config.workflow_id}")

    def _set_workflow_id(self, workflow: Workflow) -> None:
        workflow_id = f"{workflow.name}-{workflow.version}"
        self._config.set_workflow_id(workflow_id)

    def _upload_workflow_to_solver_update_checker(self, workflow: Workflow) -> None:
        workflow_config = workflow.get_workflow_config().to_json()

        payload = {
            "workflow_id": self._config.workflow_id,
            "workflow_config": workflow_config,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_solver_update_checker_client().set_value_in_table(
            SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, self._config.workflow_id, payload_json
        )

    def _get_workflow_already_deployed(self) -> bool:
        return self._endpoints.get_solver_update_checker_client().get_key_present_in_table(
            SOLVER_UPDATE_CHECKER_RESOURCE_TABLE, self._config.workflow_id
        )

    def _update_workflow_placement_decision(
        self, workflow: Workflow, staging_area_data: str, previous_instances: list[dict]
    ) -> None:
        if staging_area_data is None:
            raise RuntimeError("Staging area data is None")

        staging_area_data_json = json.loads(staging_area_data)

        if not isinstance(staging_area_data_json, dict):
            raise RuntimeError("Staging area data is not a dictionary")

        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        workflow_placement_decision = workflow.get_workflow_placement_decision_extend_staging(
            self._executor.resource_values, staging_area_data_json, previous_instances
        )
        workflow_placement_decision_json = json.dumps(workflow_placement_decision)

        self._endpoints.get_solver_update_checker_client().set_value_in_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._config.workflow_id, workflow_placement_decision_json
        )
        self._endpoints.get_solver_workflow_placement_decision_client().remove_value_from_table(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._config.workflow_id
        )

    def _upload_workflow_placement_decision(self, workflow: Workflow) -> None:
        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        workflow_placement_decision = workflow.get_workflow_placement_decision(self._executor.resource_values)
        workflow_placement_decision_json = json.dumps(workflow_placement_decision)

        self._endpoints.get_solver_update_checker_client().set_value_in_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._config.workflow_id, workflow_placement_decision_json
        )

    def _upload_workflow_to_deployer_server(self, workflow: Workflow) -> None:
        workflow_function_descriptions = workflow.get_function_description()
        workflow_function_descriptions_json = json.dumps(workflow_function_descriptions)
        deployment_config_json = self._config.to_json()
        deployed_regions = workflow.get_deployed_regions_initial_deployment()
        deployed_regions_json = json.dumps(deployed_regions)

        payload = {
            "workflow_id": self._config.workflow_id,
            "workflow_function_descriptions": workflow_function_descriptions_json,
            "deployment_config": deployment_config_json,
            "deployed_regions": deployed_regions_json,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._config.workflow_id, payload_json
        )

    def _update_workflow_to_deployer_server(
        self, workflow: Workflow, deployed_regions: dict[str, dict[str, str]]
    ) -> None:
        workflow_function_descriptions = workflow.get_function_description()
        workflow_function_descriptions_json = json.dumps(workflow_function_descriptions)
        deployment_config_json = self._config.to_json()
        deployed_regions_json = json.dumps(deployed_regions)

        payload = {
            "workflow_id": self._config.workflow_id,
            "workflow_function_descriptions": workflow_function_descriptions_json,
            "deployment_config": deployment_config_json,
            "deployed_regions": deployed_regions_json,
        }

        payload_json = json.dumps(payload)

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._config.workflow_id, payload_json
        )

    def _upload_deployment_package_resource(self, workflow: Workflow) -> None:
        deployment_packege_filename = workflow.get_deployment_packages()[0].filename

        # Append zip extension if not present
        if not deployment_packege_filename.endswith(".zip"):
            deployment_packege_filename = f"{deployment_packege_filename}.zip"

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
