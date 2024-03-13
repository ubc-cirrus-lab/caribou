from unittest.mock import Mock, patch, mock_open
from botocore.exceptions import ClientError
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.models.resource import Resource
from multi_x_serverless.deployment.common.deploy.models.workflow import Workflow
from multi_x_serverless.common.models.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.deployment.common.deploy.deployer import Deployer, DeploymentError
import unittest
import tempfile
import shutil
from multi_x_serverless.common.constants import (
    WORKFLOW_PLACEMENT_DECISION_TABLE,
    WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE,
)


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

        regions = [{"region": "region2"}]
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        workflow_builder.build_workflow.return_value = workflow
        executor.get_deployed_resources.return_value = [Resource("test_resource", "test_resource")]

        with patch.object(Deployer, "_upload_workflow_to_solver_update_checker", return_value=None), patch.object(
            Deployer, "_upload_workflow_to_deployer_server", return_value=None
        ), patch.object(Deployer, "_upload_deployment_package_resource", return_value=None), patch.object(
            Deployer, "_upload_workflow_placement_decision", return_value=None
        ), patch.object(
            Deployer, "_get_workflow_already_deployed", return_value=False
        ):
            deployer.deploy(regions)

        workflow_builder.build_workflow.assert_called_once_with(config, regions)
        deployment_packager.build.assert_called_once_with(config, workflow)
        executor.execute.assert_called_once()

    def test_deploy_with_client_error(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        regions = [{"region": "region2"}]
        workflow_builder.build_workflow.side_effect = ClientError({"Error": {}}, "operation")

        with patch.object(Deployer, "_upload_workflow_to_solver_update_checker", return_value=None), patch.object(
            Deployer, "_upload_workflow_to_deployer_server", return_value=None
        ), patch.object(Deployer, "_upload_deployment_package_resource", return_value=None):
            with self.assertRaises(DeploymentError):
                deployer.deploy(regions)

    def test_deploy_without_executor(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, None)

        regions = [{"region": "region2"}]
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        workflow_builder.build_workflow.return_value = workflow

        with patch.object(Deployer, "_upload_workflow_to_solver_update_checker", return_value=None), patch.object(
            Deployer, "_upload_workflow_to_deployer_server", return_value=None
        ), patch.object(Deployer, "_upload_deployment_package_resource", return_value=None), patch.object(
            Deployer, "_get_workflow_already_deployed", return_value=False
        ):
            with self.assertRaises(AssertionError, msg="Executor is None, this should not happen"):
                deployer.deploy(regions)

    def test_deploy_with_existing_workflow(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        regions = [{"region": "region2"}]
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        workflow_builder.build_workflow.return_value = workflow
        executor.get_deployed_resources.return_value = [Resource("test_resource", "test_resource")]

        with patch.object(
            Deployer, "_get_workflow_already_deployed", return_value=None
        ) as get_workflow_already_deployed:
            get_workflow_already_deployed.return_value = True
            with self.assertRaises(DeploymentError, msg="Workflow {} with version {} already deployed"):
                deployer.deploy(regions)

    def test_upload_workflow_to_solver_update_checker(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        workflow = Mock(spec=Workflow)
        workflow_config = Mock()
        workflow_config.to_json = Mock(return_value="test_workflow_description")
        workflow.get_workflow_config = Mock(return_value=workflow_config)

        deployer._workflow = workflow

        with patch.object(AWSRemoteClient, "set_value_in_table") as set_value_in_table:
            deployer._upload_workflow_to_solver_update_checker()

            set_value_in_table.assert_called_once_with(
                "solver_update_checker_resources_table",
                {},
                '{"workflow_id": {}, "workflow_config": "test_workflow_description"}',
            )

    def test_upload_workflow_to_deployer_server(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        workflow = Mock(spec=Workflow)
        workflow.get_function_description = Mock(return_value=[{"name": "test_function", "version": "0.0.1"}])
        workflow.get_deployed_regions_initial_deployment = Mock(return_value={"test_function": [{"region": "region2"}]})

        deployer._workflow = workflow

        with patch.object(AWSRemoteClient, "set_value_in_table") as set_value_in_table:
            deployer._upload_workflow_to_deployer_server()

            set_value_in_table.assert_called_once_with(
                "deployment_manager_resources_table",
                {},
                '{"workflow_id": {}, "workflow_function_descriptions": "[{\\"name\\": \\"test_function\\", \\"version\\": \\"0.0.1\\"}]", "deployment_config": "{}", "deployed_regions": "{\\"test_function\\": [{\\"region\\": \\"region2\\"}]}"}',
            )

    def test_upload_deployment_package_resource(self):
        config = Config({"workflow_id": "test_id"}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        workflow = Mock(spec=Workflow)
        deployment_package = Mock(filename="test_deployment_package")
        workflow.get_deployment_packages = Mock(return_value=[deployment_package])

        deployer._workflow = workflow

        with patch.object(AWSRemoteClient, "upload_resource") as upload_resource:
            with patch("builtins.open", mock_open(read_data=b"test_deployment_package")) as open_file:
                deployer._upload_deployment_package_resource()

            upload_resource.assert_called_once_with("deployment_package_test_id", b"test_deployment_package")

    def test_get_workflow_already_deployed(self):
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        deployer._endpoints.get_deployment_algorithm_update_checker_client().get_key_present_in_table = Mock(
            return_value=True
        )
        result = deployer._get_workflow_already_deployed()
        self.assertTrue(result)

    def test_filter_function_to_deployment_regions(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        function_to_deployment_regions = {
            "test_function": {"region": "region2"},
            "new_function": {"region": "region3"},
        }
        deployed_regions = {"test_function": {"region": "region2"}}

        expected_filtered_regions = {
            "new_function": {"region": "region3"},
        }

        filtered_regions = deployer._filter_function_to_deployment_regions(
            function_to_deployment_regions, deployed_regions
        )

        self.assertEqual(filtered_regions, expected_filtered_regions)


if __name__ == "__main__":
    unittest.main()
