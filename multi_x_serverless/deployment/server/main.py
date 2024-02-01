import json
import sys
import traceback

from multi_x_serverless.common.constants import DEPLOYMENT_MANAGER_RESOURCE_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.deployer import Deployer, create_default_deployer
from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory


def main() -> int:
    try:
        input_workflow_id = sys.argv[1]

        run(
            input_workflow_id=input_workflow_id,
        )

        return 0
    except Exception:  # pylint: disable=broad-except
        print(traceback.format_exc(), file=sys.stderr)
        return 2


def run(
    input_workflow_id: str,
) -> None:
    endpoints = Endpoints()
    deployment_manager_client = endpoints.get_deployment_manager_client()

    workflow_data = deployment_manager_client.get_value_from_table(DEPLOYMENT_MANAGER_RESOURCE_TABLE, input_workflow_id)

    workflow_data = json.loads(workflow_data)

    if not isinstance(workflow_data, dict):
        raise ValueError("Workflow data is not a dict")

    workflow_function_descriptions = json.loads(workflow_data["workflow_function_descriptions"])
    deployment_config = json.loads(workflow_data["deployment_config"])
    deployed_regions = json.loads(workflow_data["deployed_regions"])

    if not isinstance(deployment_config, dict):
        raise ValueError("Deployment config is not a dict")

    if not isinstance(workflow_function_descriptions, list):
        raise ValueError("Workflow function description is not a list")

    run_deployer(
        deployment_config=deployment_config,
        workflow_function_descriptions=workflow_function_descriptions,
        deployed_regions=deployed_regions,
    )


def run_deployer(
    deployment_config: dict,
    workflow_function_descriptions: list[dict],
    deployed_regions: dict[str, dict[str, str]],
) -> None:
    deployer_factory = DeployerFactory(project_dir=None)
    config: Config = deployer_factory.create_config_obj_from_dict(deployment_config=deployment_config)
    deployer: Deployer = create_default_deployer(config=config)
    deployer.re_deploy(
        workflow_function_descriptions=workflow_function_descriptions,
        deployed_regions=deployed_regions,
    )


if __name__ == "__main__":
    sys.exit(main())
