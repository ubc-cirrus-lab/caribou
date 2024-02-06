import os
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.deployer import Deployer
from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory


def main():
    print("Test deploying the application.")

    project_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "integrationtest_workflow")

    deployer_factory = DeployerFactory(project_dir)
    config: Config = deployer_factory.create_config_obj()
    deployer: Deployer = deployer_factory.create_deployer(config=config)

    deployer.deploy(config.home_regions)


if __name__ == "__main__":
    main()
