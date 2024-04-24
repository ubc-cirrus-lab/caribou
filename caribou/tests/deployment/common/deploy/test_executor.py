import unittest
from unittest.mock import Mock, patch, MagicMock
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.models.deployment_plan import DeploymentPlan
from caribou.deployment.common.deploy.models.instructions import APICall, RecordResourceVariable
from caribou.deployment.common.deploy.models.resource import Resource
from caribou.deployment.common.deploy.models.variable import Variable
from caribou.deployment.common.deploy.executor import Executor
from caribou.common.models.remote_client.remote_client import RemoteClient

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
            "provider1:region2": [APICall("test_method", {"param1": Variable("var1")}, "output_var")]
        }
        self.executor.variables = {"var1": "value1"}

        with patch(
            "caribou.common.models.remote_client.remote_client_factory.RemoteClientFactory.get_remote_client"
        ) as mock_get_remote_client:
            mock_client = Mock(spec=RemoteClient)
            mock_client.test_method = Mock()
            mock_client.test_method.return_value = "response"
            mock_get_remote_client.return_value = mock_client

            self.executor.execute(deployment_plan)

            mock_get_remote_client.assert_called_once_with("provider1", "region2")
            mock_client.test_method.assert_called_once_with(param1="value1")
            self.assertEqual(self.executor.variables["output_var"], "response")

    def test_execute_apicall_with_client_error(self):
        deployment_plan = DeploymentPlan()
        deployment_plan.instructions = {
            "aws:us-west-1": [APICall("test_method", {"param1": Variable("var1")}, "output_var")]
        }
        self.executor.variables = {"var1": "value1"}

        with patch(
            "caribou.common.models.remote_client.remote_client_factory.RemoteClientFactory.get_remote_client"
        ) as mock_get_remote_client:
            mock_client = Mock(spec=RemoteClient)
            mock_client.test_method = Mock()
            mock_client.test_method.side_effect = RuntimeError("test error")
            mock_get_remote_client.return_value = mock_client

            with self.assertRaises(RuntimeError):
                self.executor.execute(deployment_plan)

    @patch("caribou.common.models.remote_client.remote_client_factory.RemoteClientFactory.get_remote_client")
    def test_execute_recordresourcevariable(self, mock_get_remote_client):
        # Create a mock client and set its create_sync_tables method to a MagicMock
        mock_client = MagicMock()
        mock_get_remote_client.return_value = mock_client

        deployment_plan = DeploymentPlan()
        deployment_plan.instructions = {
            "aws:us-west-1": [RecordResourceVariable("var_name", "resource_type", "resource_name", "var_value")]
        }
        self.executor.variables = {"var_value": "value1"}

        self.executor.execute(deployment_plan)

        expected_payload = {"name": "resource_name", "resource_type": "resource_type", "var_name": "value1"}
        self.assertEqual(self.executor.resource_values["resource_type"], [expected_payload])

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
