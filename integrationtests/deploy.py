import os
import tempfile
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.deployer import Deployer
from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory
from multi_x_serverless.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient
import shutil


def test_deploy():
    print("Test deploying the application.")

    workdir = tempfile.mkdtemp()

    try:

        test_database_path = os.path.join(workdir, "test_database.sqlite")

        os.environ["MULTI_X_SERVERLESS_INTEGRATION_TEST_DB_PATH"] = test_database_path

        project_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "integration_test_workflow")

        deployer_factory = DeployerFactory(project_dir)
        config: Config = deployer_factory.create_config_obj()
        deployer: Deployer = deployer_factory.create_deployer(config=config)

        deployer.deploy(config.home_regions)

        remote_client = IntegrationTestRemoteClient()

        # Check that the functions are deployed
        deployed_functions = remote_client.get_all_values_from_table("functions")

        assert len(deployed_functions) == 7
        

    finally:
        shutil.rmtree(workdir)


if __name__ == "__main__":
    test_deploy()
