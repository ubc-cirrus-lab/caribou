import json
import logging
from typing import Any

from caribou.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
    WORKFLOW_PLACEMENT_DECISION_TABLE,
    WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE,
)
from caribou.common.models.endpoints import Endpoints
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployer import Deployer, create_default_deployer
from caribou.deployment.common.factories.deployer_factory import DeployerFactory

logger = logging.getLogger(__name__)


class ReDeploymentUtility:
    def __init__(self, input_workflow_id: str) -> None:
        self._workflow_id = input_workflow_id
        self._endpoints = Endpoints()
        self._time_keys_to_instances: dict[str, Any] = {}
        self._workflow_data = self._load_workflow_data()

        self._check_workflow_already_deployed()

    def _load_workflow_data(self) -> dict[str, Any]:
        workflow_data = self._endpoints.get_deployment_manager_client().get_value_from_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._workflow_id
        )

        workflow_data = json.loads(workflow_data)

        if not isinstance(workflow_data, dict):
            raise ValueError("Workflow data is not a dict")

        return workflow_data

    def run(
        self,
    ) -> None:
        workflow_function_descriptions = json.loads(self._workflow_data["workflow_function_descriptions"])
        deployment_config = json.loads(self._workflow_data["deployment_config"])
        deployed_regions = json.loads(self._workflow_data["deployed_regions"])

        if not isinstance(deployment_config, dict):
            raise ValueError("Deployment config is not a dict")

        if not isinstance(workflow_function_descriptions, list):
            raise ValueError("Workflow function description is not a list")

        staging_area_data_raw = self._endpoints.get_deployment_manager_client().get_value_from_table(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._workflow_id
        )

        if staging_area_data_raw is None:
            raise RuntimeError("Staging area data is None")

        staging_area_data = json.loads(staging_area_data_raw)

        for time_key, specific_staging_area_data in staging_area_data["time_keys_to_staging_area_data"].items():
            logger.info("Running deployer for workflow: %s and time key: %s", self._workflow_id, time_key)
            self._run_deployer(
                deployment_config=deployment_config,
                workflow_function_descriptions=workflow_function_descriptions,
                deployed_regions=deployed_regions,
                specific_staging_area_data=specific_staging_area_data,
                time_key=time_key,
            )

        self._update_workflow_placement_decision(staging_area_data["expiry_time"])

    def _run_deployer(
        self,
        deployment_config: dict,
        workflow_function_descriptions: list[dict],
        deployed_regions: dict[str, dict[str, str]],
        specific_staging_area_data: dict[str, Any],
        time_key: str,
    ) -> None:
        deployer_factory = DeployerFactory(project_dir=None)
        config: Config = deployer_factory.create_config_obj_from_dict(deployment_config=deployment_config)
        deployer: Deployer = create_default_deployer(config=config)
        new_deployment_instances = deployer.re_deploy(
            workflow_function_descriptions=workflow_function_descriptions,
            deployed_regions=deployed_regions,
            specific_staging_area_data=specific_staging_area_data,
        )

        self._time_keys_to_instances[time_key] = new_deployment_instances

        self._upload_new_deployed_regions(deployed_regions)

    def _update_workflow_placement_decision(
        self,
        expiry_time: str,
    ) -> None:
        previous_workflow_placement_decision_raw = self._endpoints.get_deployment_manager_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._workflow_id
        )

        if previous_workflow_placement_decision_raw is None:
            raise RuntimeError("Current workflow placement decision is None")

        previous_workflow_placement_decision = json.loads(previous_workflow_placement_decision_raw)

        previous_workflow_placement_decision["workflow_placement"]["current_deployment"] = {
            "expiry_time": expiry_time,
            "time_keys": list(self._time_keys_to_instances.keys()),
            "instances": self._time_keys_to_instances,
        }

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, self._workflow_id, json.dumps(previous_workflow_placement_decision)
        )

        self._endpoints.get_deployment_manager_client().remove_key(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._workflow_id
        )

    def _upload_new_deployed_regions(self, deployed_regions: dict[str, dict[str, str]]) -> None:
        self._workflow_data["deployed_regions"] = json.dumps(deployed_regions)
        payload_json = json.dumps(self._workflow_data)

        self._endpoints.get_deployment_manager_client().set_value_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._workflow_id, payload_json
        )

    def _check_workflow_already_deployed(self) -> None:
        deployed = self._endpoints.get_deployment_manager_client().get_key_present_in_table(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, self._workflow_id
        )
        if not deployed:
            raise RuntimeError("Workflow is not deployed")
