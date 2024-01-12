import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from multi_x_serverless.deployment.common.config import Config
from multi_x_serverless.deployment.common.deploy.models.resource import Resource
from multi_x_serverless.deployment.common.deploy.models.workflow import Workflow
from multi_x_serverless.deployment.common.deploy.deployer import Deployer, DeploymentError
import unittest
import tempfile
import shutil
import os


class TestDeployer(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_deploy_without_deployment_config(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        regions = [{"region": "us-west-1"}]
        workflow = Workflow("test_workflow", [], [], [], config)
        workflow_builder.build_workflow.return_value = workflow
        executor.get_deployed_resources.return_value = [Resource("test_resource", "test_resource")]

        result = deployer.deploy(regions)

        workflow_builder.build_workflow.assert_called_once_with(config, regions)
        deployment_packager.build.assert_called_once_with(config, workflow)
        executor.execute.assert_called_once()
        self.assertEqual(result, [Resource("test_resource", "test_resource")])

    def test_deploy_with_client_error(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        regions = [{"region": "us-west-1"}]
        workflow_builder.build_workflow.side_effect = ClientError({"Error": {}}, "operation")

        with self.assertRaises(DeploymentError):
            deployer.deploy(regions)

    def test_deploy_without_executor(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, None)

        regions = [{"region": "us-west-1"}]
        workflow = Workflow("test_workflow", [], [], [], config)
        workflow_builder.build_workflow.return_value = workflow

        with self.assertRaises(RuntimeError, msg="Cannot deploy with deletion deployer"):
            deployer.deploy(regions)


if __name__ == "__main__":
    unittest.main()
