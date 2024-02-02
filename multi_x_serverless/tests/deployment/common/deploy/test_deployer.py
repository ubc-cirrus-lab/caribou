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
import json
from multi_x_serverless.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
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
            with self.assertRaises(RuntimeError, msg="Cannot deploy with deletion deployer"):
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

    def test_re_deploy_without_executor(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, None)

        function_to_deployment_regions = {"test_function": [{"provider": "provider1", "region": "region1"}]}
        workflow_function_description = [{"name": "test_function", "version": "0.0.1"}]
        deployed_regions = {"test_function": [{"region": "region2"}]}
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        workflow_builder.re_build_workflow.return_value = workflow

        with patch.object(
            Deployer, "_get_workflow_already_deployed", return_value=None
        ) as get_workflow_already_deployed:
            get_workflow_already_deployed.return_value = True
            with self.assertRaises(RuntimeError, msg="Cannot deploy with deletion deployer"):
                deployer.re_deploy(function_to_deployment_regions, workflow_function_description, deployed_regions)

    def test_re_deploy_workflow_not_deployed(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        function_to_deployment_regions = {"test_function": [{"region": "region1"}]}
        workflow_function_description = [{"name": "test_function", "version": "0.0.1"}]
        deployed_regions = {"test_function": [{"region": "region2"}]}

        with patch.object(
            Deployer, "_get_workflow_already_deployed", return_value=None
        ) as get_workflow_already_deployed:
            get_workflow_already_deployed.return_value = False
            with self.assertRaises(
                DeploymentError, msg="Workflow {} with version {} not deployed, something went wrong"
            ):
                deployer.re_deploy(function_to_deployment_regions, workflow_function_description, deployed_regions)

    def test_re_deploy_with_client_error(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)
        deployed_regions = {"test_function": [{"region": "region2"}]}

        function_to_deployment_regions = {"test_function": [{"region": "region1"}]}
        workflow_function_description = [{"name": "test_function", "version": "0.0.1"}]

        with patch.object(Deployer, "_re_deploy", side_effect=ClientError({}, "TestOperation")):
            with self.assertRaises(DeploymentError):
                deployer.re_deploy(function_to_deployment_regions, workflow_function_description, deployed_regions)

    def test_re_deploy(self):
        config = Config({}, self.test_dir)
        mock_workflow = Mock()
        workflow_builder = Mock()
        workflow_builder.re_build_workflow.return_value = mock_workflow
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)
        function_to_deployment_regions = {"test_function": {"region": "region1"}}
        workflow_function_descriptions = [{"name": "test_function", "version": "0.0.1"}]
        deployed_regions = {"test_function": {"region": "region2"}}
        deployer._get_workflow_already_deployed = Mock(return_value=True)
        deployer._filter_function_to_deployment_regions = Mock(return_value=function_to_deployment_regions)
        deployer._merge_deployed_regions = Mock(return_value=deployed_regions)
        deployer._update_workflow_to_deployer_server = Mock()
        deployer._update_workflow_placement_decision = Mock()

        deployer._re_deploy(function_to_deployment_regions, workflow_function_descriptions, deployed_regions)

        workflow_builder.re_build_workflow.assert_called_once_with(
            config, function_to_deployment_regions, workflow_function_descriptions, deployed_regions
        )
        deployment_packager.re_build.assert_called_once()
        executor.execute.assert_called_once()
        deployer._update_workflow_to_deployer_server.assert_called_once()
        deployer._update_workflow_placement_decision.assert_called_once()

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

        with patch.object(AWSRemoteClient, "set_value_in_table") as set_value_in_table:
            deployer._upload_workflow_to_solver_update_checker(workflow, "test_workflow_id")

            set_value_in_table.assert_called_once_with(
                "solver_update_checker_resources_table",
                "test_workflow_id",
                '{"workflow_id": "test_workflow_id", "workflow_config": "test_workflow_description"}',
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

        with patch.object(AWSRemoteClient, "set_value_in_table") as set_value_in_table:
            deployer._upload_workflow_to_deployer_server(workflow)

            set_value_in_table.assert_called_once_with(
                "deployment_manager_resources_table",
                "test_workflow_id",
                '{"workflow_id": "test_workflow_id", "workflow_function_descriptions": "[{\\"name\\": \\"test_function\\", \\"version\\": \\"0.0.1\\"}]", "deployment_config": "{}", "deployed_regions": "{\\"test_function\\": [{\\"region\\": \\"region2\\"}]}"}',
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

        with patch.object(AWSRemoteClient, "upload_resource") as upload_resource:
            with patch("builtins.open", mock_open(read_data=b"test_deployment_package")) as open_file:
                deployer._upload_deployment_package_resource(workflow)

            upload_resource.assert_called_once_with("deployment_package_test_id", b"test_deployment_package")

    def test_get_workflow_already_deployed(self):
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        deployer._endpoints.get_solver_update_checker_client().get_key_present_in_table = Mock(return_value=True)
        result = deployer._get_workflow_already_deployed("workflow_id")
        self.assertTrue(result)

    def test_update_workflow_placement_decision(self):
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        mock_workflow = Mock()
        deployer._endpoints.get_solver_workflow_placement_decision_client().get_value_from_table = Mock(
            return_value='{"key": "value"}'
        )
        mock_workflow.get_workflow_placement_decision_extend_staging = Mock(return_value={"key": "value"})
        deployer._endpoints.get_solver_update_checker_client().set_value_in_table = Mock()
        deployer._update_workflow_placement_decision(mock_workflow)
        deployer._endpoints.get_solver_update_checker_client().set_value_in_table.assert_called_once()

    def test_upload_workflow_placement_decision(self):
        config = Mock()
        config.workflow_id = "workflow_id"
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        mock_workflow = Mock()
        mock_workflow.get_workflow_placement_decision = Mock(return_value={"key": "value"})
        deployer._endpoints.get_solver_update_checker_client().set_value_in_table = Mock()
        deployer._upload_workflow_placement_decision(mock_workflow)
        deployer._endpoints.get_solver_update_checker_client().set_value_in_table.assert_called_once()

    def test_update_workflow_to_deployer_server(self):
        config = Mock()
        config.workflow_id = "workflow_id"
        config.to_json = Mock(return_value='{"config_key": "config_value"}')
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        mock_workflow = Mock()
        mock_workflow.get_function_description = Mock(return_value={"function_key": "function_value"})
        deployed_regions = {"region_key": {"region_sub_key": "region_sub_value"}}

        deployer._endpoints.get_deployment_manager_client().set_value_in_table = Mock()
        deployer._update_workflow_to_deployer_server(mock_workflow, "workflow_id", deployed_regions)

        expected_payload = {
            "workflow_id": "workflow_id",
            "workflow_function_descriptions": json.dumps({"function_key": "function_value"}),
            "deployment_config": '{"config_key": "config_value"}',
            "deployed_regions": json.dumps({"region_key": {"region_sub_key": "region_sub_value"}}),
        }

        deployer._endpoints.get_deployment_manager_client().set_value_in_table.assert_called_once_with(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE, "workflow_id", json.dumps(expected_payload)
        )

    def test_merge_deployed_regions(self):
        config = Config({}, self.test_dir)
        workflow_builder = Mock()
        deployment_packager = Mock()
        executor = Mock()
        deployer = Deployer(config, workflow_builder, deployment_packager, executor)

        deployed_regions = {"test_function": [{"region": "region2"}]}
        filtered_function_to_deployment_regions = {
            "test_function": {"region": "region1"},
            "new_function": {"region": "region3"},
        }

        expected_merged_regions = {
            "test_function": {"region": "region1"},
            "new_function": {"region": "region3"},
        }

        merged_regions = deployer._merge_deployed_regions(deployed_regions, filtered_function_to_deployment_regions)

        self.assertEqual(merged_regions, expected_merged_regions)

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
