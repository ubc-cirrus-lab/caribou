import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.deployment.common.config import Config
from multi_x_serverless.deployment.common.deploy.models.deployment_plan import DeploymentPlan
from multi_x_serverless.deployment.common.deploy.models.instructions import APICall, RecordResourceVariable
from multi_x_serverless.deployment.common.deploy.models.resource import Resource
from multi_x_serverless.deployment.common.deploy.models.variable import Variable
from multi_x_serverless.deployment.common.deploy.executor import Executor
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient

import tempfile
import shutil


class TestExecutor(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = Config({}, self.test_dir)
        self.executor = Executor(self.config)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_execute_apicall(self):
        deployment_plan = DeploymentPlan()
        deployment_plan.instructions = {
            "aws:us-west-1": [APICall("test_method", {"param1": Variable("var1")}, "output_var")]
        }
        self.executor.variables = {"var1": "value1"}

        with patch(
            "multi_x_serverless.deployment.common.factories.remote_client_factory.RemoteClientFactory.get_remote_client"
        ) as mock_get_remote_client:
            mock_client = Mock(spec=RemoteClient)
            mock_client.test_method = Mock()
            mock_client.test_method.return_value = "response"
            mock_get_remote_client.return_value = mock_client

            self.executor.execute(deployment_plan)

            mock_get_remote_client.assert_called_once_with("aws", "us-west-1")
            mock_client.test_method.assert_called_once_with(param1="value1")
            self.assertEqual(self.executor.variables["output_var"], "response")

    def test_execute_apicall_with_client_error(self):
        deployment_plan = DeploymentPlan()
        deployment_plan.instructions = {
            "aws:us-west-1": [APICall("test_method", {"param1": Variable("var1")}, "output_var")]
        }
        self.executor.variables = {"var1": "value1"}

        with patch(
            "multi_x_serverless.deployment.common.factories.remote_client_factory.RemoteClientFactory.get_remote_client"
        ) as mock_get_remote_client:
            mock_client = Mock(spec=RemoteClient)
            mock_client.test_method = Mock()
            mock_client.test_method.side_effect = RuntimeError("test error")
            mock_get_remote_client.return_value = mock_client

            with self.assertRaises(RuntimeError):
                self.executor.execute(deployment_plan)

    def test_execute_recordresourcevariable(self):
        deployment_plan = DeploymentPlan()
        deployment_plan.instructions = {
            "aws:us-west-1": [RecordResourceVariable("var_name", "resource_type", "resource_name", "var_value")]
        }
        self.executor.variables = {"var_value": "value1"}

        self.executor.execute(deployment_plan)

        expected_payload = {"name": "resource_name", "resource_type": "resource_type", "var_name": "value1"}
        self.assertEqual(self.executor.resource_values["resource_type"], [expected_payload])

    def test_get_deployed_resources(self):
        self.executor.resource_values = {
            "resource_type1": [{"name": "resource_name1", "resource_type": "resource_type1", "var_name": "value1"}]
        }

        deployed_resources = self.executor.get_deployed_resources()

        expected_resource = Resource(
            "resource_type1", {"name": "resource_name1", "resource_type": "resource_type1", "var_name": "value1"}
        )
        self.assertEqual(deployed_resources, [expected_resource])

    def test_default_handler(self):
        with self.assertRaises(RuntimeError):
            self.executor._default_handler("instruction", "client")

    def test_resolve_variables(self):
        call = APICall("test_method", {"param1": Variable("var1")}, "output_var")
        self.executor.variables = {"var1": "value1"}

        final_kwargs = self.executor._Executor__resolve_variables(call)

        self.assertEqual(final_kwargs, {"param1": "value1"})


if __name__ == "__main__":
    unittest.main()
