import sys
import traceback

from multi_x_serverless.deployment.common.config import Config
from multi_x_serverless.deployment.common.deploy.deployer import Deployer, create_default_deployer
from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory


def main() -> int:
    try:
        run_deployer(deployment_config={}, regions=[], workflow_function_description={})
        return 0
    except Exception:  # pylint: disable=broad-except
        print(traceback.format_exc(), file=sys.stderr)
        return 2


def run_deployer(deployment_config: dict, regions: list[dict[str, str]], workflow_function_description: dict) -> None:
    deployer_factory = DeployerFactory(project_dir=None)
    config: Config = deployer_factory.create_config_obj_from_dict(deployment_config=deployment_config)
    deployer: Deployer = create_default_deployer(config=config)
    deployer.re_deploy(regions, workflow_function_description)


if __name__ == "__main__":
    sys.exit(main())
