from unittest.mock import Mock, patch, mock_open
from botocore.exceptions import ClientError
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.models.resource import Resource
from caribou.deployment.common.deploy.models.workflow import Workflow
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.deployment.common.deploy.deployer import Deployer, DeploymentError
from caribou.deployment.common.deploy.models.deployment_plan import DeploymentPlan
import unittest
import tempfile
import shutil
import json
from caribou.common.constants import (
    WORKFLOW_PLACEMENT_DECISION_TABLE,
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

        with patch.object(
            Deployer, "_upload_workflow_to_deployment_optimization_monitor", return_value=None
        ), patch.object(Deployer, "_upload_workflow_to_deployer_server", return_value=None), patch.object(
            Deployer, "_upload_deployment_package_resource", return_value=None
        ), patch.object(
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

        with patch.object(
            Deployer, "_upload_workflow_to_deployment_optimization_monitor", return_value=None
        ), patch.object(Deployer, "_upload_workflow_to_deployer_server", return_value=None), patch.object(
            Deployer, "_upload_deployment_package_resource", return_value=None
        ):
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

        with patch.object(
            Deployer, "_upload_workflow_to_deployment_optimization_monitor", return_value=None
        ), patch.object(Deployer, "_upload_workflow_to_deployer_server", return_value=None), patch.object(
            Deployer, "_upload_deployment_package_resource", return_value=None
        ), patch.object(
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

    def test_upload_workflow_to_deployment_optimization_monitor(self):
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
            deployer._upload_workflow_to_deployment_optimization_monitor()

            set_value_in_table.assert_called_once_with(
                "deployment_optimization_monitor_resource_table",
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

        deployer._endpoints.get_deployment_optimization_monitor_client().get_key_present_in_table = Mock(
            return_value=True
        )
        result = deployer._get_workflow_already_deployed()
        self.assertTrue(result)

    @patch.object(Deployer, "_re_deploy")
    def test_re_deploy(self, mock_re_deploy):
        # Set up the Deployer object
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        # Set up the mock
        mock_re_deploy.return_value = "mocked_result"

        # Call the method
        workflow_function_descriptions = [{}]
        deployed_regions = {"region_name": {"key": "value"}}
        specific_staging_area_data = {"key": "value"}
        result = deployer.re_deploy(workflow_function_descriptions, deployed_regions, specific_staging_area_data)

        # Check that the result is correct
        self.assertEqual(result, "mocked_result")

        # Check that the mock was called with the correct arguments
        mock_re_deploy.assert_called_once_with(
            workflow_function_descriptions, deployed_regions, specific_staging_area_data
        )

    @patch.object(Deployer, "_get_function_to_deployment_regions")
    @patch.object(Deployer, "_filter_function_to_deployment_regions")
    @patch.object(Deployer, "_update_deployed_regions")
    @patch.object(Deployer, "_get_new_deployment_instances")
    def test_re_deploy(
        self,
        mock_get_new_deployment_instances,
        mock_update_deployed_regions,
        mock_filter_function_to_deployment_regions,
        mock_get_function_to_deployment_regions,
    ):
        # Set up the Deployer object
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        # Create a mock for the workflow
        mock_workflow = Mock()
        mock_workflow.get_deployment_instructions.return_value = "deployment_instructions"

        # Set up the mocks
        mock_get_function_to_deployment_regions.return_value = "function_to_deployment_regions"
        mock_filter_function_to_deployment_regions.return_value = "filtered_function_to_deployment_regions"
        workflow_builder.re_build_workflow.return_value = mock_workflow
        deployment_packager.re_build.return_value = "deployment_packager"
        executor.execute.return_value = "executor"
        mock_get_new_deployment_instances.return_value = "new_deployment_instances"

        # Call the method
        workflow_function_descriptions = [{}]
        deployed_regions = {"region_name": {"key": "value"}}
        specific_staging_area_data = {"key": "value"}
        result = deployer._re_deploy(workflow_function_descriptions, deployed_regions, specific_staging_area_data)

        # Check that the result is correct
        self.assertEqual(result, "new_deployment_instances")

        # Check that the mocks were called with the correct arguments
        mock_get_function_to_deployment_regions.assert_called_once_with(specific_staging_area_data)
        mock_filter_function_to_deployment_regions.assert_called_once_with(
            "function_to_deployment_regions", deployed_regions
        )
        workflow_builder.re_build_workflow.assert_called_once_with(
            config, "filtered_function_to_deployment_regions", workflow_function_descriptions, deployed_regions
        )
        deployment_packager.re_build.assert_called_once_with(
            mock_workflow, deployer._endpoints.get_deployment_manager_client()
        )
        executor.execute.assert_called_once_with(DeploymentPlan(mock_workflow.get_deployment_instructions()))
        mock_update_deployed_regions.assert_called_once_with(deployed_regions)
        mock_get_new_deployment_instances.assert_called_once_with(specific_staging_area_data)

    def test_get_function_to_deployment_regions(self):
        # Set up the Deployer object
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        # Call the method
        staging_area_data = {
            "instance_name:detail": {
                "provider_region": {
                    "provider": "provider",
                    "region": "region",
                }
            }
        }
        result = deployer._get_function_to_deployment_regions(staging_area_data)

        # Check that the result is correct
        expected_result = {
            "instance_name_provider-region": {
                "provider": "provider",
                "region": "region",
            }
        }
        self.assertEqual(result, expected_result)

    def test_get_new_deployment_instances(self):
        # Set up the Deployer object
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        # Create a mock for the _workflow attribute
        mock_workflow = Mock()
        deployer._workflow = mock_workflow

        # Set up the mock
        mock_workflow.get_deployment_instances.return_value = "deployment_instances"

        # Call the method
        staging_area_data = {"key": "value"}
        result = deployer._get_new_deployment_instances(staging_area_data)

        # Check that the result is correct
        self.assertEqual(result, "deployment_instances")

        # Check that the mock was called with the correct arguments
        mock_workflow.get_deployment_instances.assert_called_once_with(staging_area_data)

    def test_upload_workflow_placement_decision(self):
        # Set up the Deployer object
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        # Create a mock for the _workflow attribute
        mock_workflow = Mock()
        deployer._workflow = mock_workflow

        # Create a mock for the _endpoints attribute
        mock_endpoints = Mock()
        deployer._endpoints = mock_endpoints

        # Create a mock for the get_deployment_optimization_monitor_client method
        mock_client = Mock()
        mock_endpoints.get_deployment_optimization_monitor_client.return_value = mock_client

        # Set up the mocks
        mock_workflow.get_workflow_placement_decision_initial_deployment.return_value = "workflow_placement_decision"
        mock_client.set_value_in_table.return_value = "set_value_in_table"

        # Call the method
        deployer._upload_workflow_placement_decision()

        # Check that the mocks were called with the correct arguments
        mock_workflow.get_workflow_placement_decision_initial_deployment.assert_called_once()
        mock_client.set_value_in_table.assert_called_once_with(
            WORKFLOW_PLACEMENT_DECISION_TABLE, config.workflow_id, json.dumps("workflow_placement_decision")
        )

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
