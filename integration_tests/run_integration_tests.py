from integration_tests.run_test_deploy import test_deploy
from integration_tests.run_test_data_collect import test_data_collect
from integration_tests.run_function import test_run_function
import tempfile
import os
import shutil
import sys


def main(workflow_dir: str):
    workdir = tempfile.mkdtemp()

    test_database_path = os.path.join(workdir, "test_database.sqlite")

    os.environ["MULTI_X_SERVERLESS_INTEGRATION_TEST_DB_PATH"] = test_database_path
    os.environ["INTEGRATIONTEST_ON"] = "True"

    try:
        test_deploy(workflow_dir)
        test_data_collect()
        test_run_function(workdir)
    finally:
        shutil.rmtree(workdir)
        os.environ.pop("MULTI_X_SERVERLESS_INTEGRATION_TEST_DB_PATH")
        os.environ.pop("INTEGRATIONTEST_ON")

        deployment_package_folder = os.path.join(workflow_dir, ".multi_x_serverless", "deployment_package")
        if os.path.exists(deployment_package_folder):
            shutil.rmtree(deployment_package_folder)


if __name__ == "__main__":

    workflow_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "integration_test_workflow")

    if len(sys.argv) > 1:
        workflow_dir = sys.argv[1]

    assert os.path.exists(workflow_dir), f"Workflow directory {workflow_dir} does not exist."

    main(workflow_dir)
