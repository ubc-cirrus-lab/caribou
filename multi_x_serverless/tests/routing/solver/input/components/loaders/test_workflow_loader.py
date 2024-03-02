import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loaders.workflow_loader import WorkflowLoader


class TestWorkflowLoader(unittest.TestCase):
    def setUp(self):
        self.instances_data = [{"instance_name": "instance_1", "regions_and_providers": {"providers": {}}}]
        self.workflow_data = {
            "instance_1": {
                "favourite_home_region": "provider_1:region_1",
                "favourite_home_region_average_runtime": 26.0,
                "favourite_home_region_tail_runtime": 31.0,
                "projected_monthly_invocations": 12.5,
                "execution_summary": {
                    "provider_1:region_1": {"average_runtime": 26.0, "tail_runtime": 31.0},
                    "provider_1:region_2": {"average_runtime": 26.0, "tail_runtime": 31.0},
                },
                "invocation_summary": {
                    "instance_2": {
                        "probability_of_invocation": 0.8,
                        "average_data_transfer_size": 0.0007,
                        "transmission_summary": {
                            "provider_1:region_1": {
                                "provider_1:region_1": {"average_latency": 0.00125, "tail_latency": 0.00175},
                                "provider_1:region_2": {"average_latency": 0.125, "tail_latency": 0.155},
                            },
                            "provider_1:region_2": {
                                "provider_1:region_1": {"average_latency": 0.095, "tail_latency": 0.125}
                            },
                        },
                    }
                },
            },
            "instance_2": {
                "favourite_home_region": "provider_1:region_1",
                "favourite_home_region_average_runtime": 12.5,
                "favourite_home_region_tail_runtime": 12.5,
                "projected_monthly_invocations": 11.25,
                "execution_summary": {
                    "provider_1:region_1": {"average_runtime": 12.5, "tail_runtime": 12.5},
                    "provider_1:region_2": {"average_runtime": 12.5, "tail_runtime": 12.5},
                },
                "invocation_summary": {},
            },
        }
        self.client = Mock(spec=RemoteClient)
        self.loader = WorkflowLoader(self.client, self.instances_data)

    @patch(
        "multi_x_serverless.routing.solver.input.components.loaders.workflow_loader.WorkflowLoader._retrieve_workflow_data"
    )
    def test_setup(self, mock_retrieve_workflow_data):
        mock_retrieve_workflow_data.return_value = self.workflow_data
        self.loader.setup({"aws:region1"})
        self.assertEqual(self.loader._workflow_data, self.workflow_data)

    def test_get_runtime(self):
        self.loader._workflow_data = self.workflow_data
        runtime = self.loader.get_runtime("instance_1", "provider_1:region_1")
        self.assertEqual(runtime, 26.0)

    def test_get_latency(self):
        self.loader._workflow_data = self.workflow_data
        latency = self.loader.get_latency("instance_1", "instance_2", "provider_1:region_1", "provider_1:region_1")
        self.assertEqual(latency, 0.00125)

    def test_get_data_transfer_size(self):
        self.loader._workflow_data = self.workflow_data
        data_transfer_size = self.loader.get_data_transfer_size("instance_1", "instance_2")
        self.assertEqual(data_transfer_size, 0.0007)

    def test_get_invocation_probability(self):
        self.loader._workflow_data = self.workflow_data
        invocation_probability = self.loader.get_invocation_probability("instance_1", "instance_2")
        self.assertEqual(invocation_probability, 0.8)

    def test_get_favourite_region(self):
        self.loader._workflow_data = self.workflow_data
        favourite_region = self.loader.get_favourite_region("instance_1")
        self.assertEqual(favourite_region, "provider_1:region_1")

    def test_get_favourite_region_runtime(self):
        self.loader._workflow_data = self.workflow_data
        favourite_region_runtime = self.loader.get_favourite_region_runtime("instance_1")
        self.assertEqual(favourite_region_runtime, 26.0)

    def test_get_all_favorite_regions(self):
        self.loader._workflow_data = self.workflow_data
        all_favorite_regions = self.loader.get_all_favorite_regions()
        self.assertEqual(all_favorite_regions, {"provider_1:region_1"})

    def test_get_projected_monthly_invocations(self):
        self.loader._workflow_data = self.workflow_data
        projected_monthly_invocations = self.loader.get_projected_monthly_invocations("instance_1")
        self.assertEqual(projected_monthly_invocations, 12.5)

    def test_get_vcpu(self):
        self.loader._instances_regions_and_providers = {"instance_1": {"provider_1": {"config": {"vcpu": 2}}}}
        vcpu = self.loader.get_vcpu("instance_1", "provider_1")
        self.assertEqual(vcpu, 2)

    def test_get_memory(self):
        self.loader._instances_regions_and_providers = {"instance_1": {"provider_1": {"config": {"memory": 1024}}}}
        memory = self.loader.get_memory("instance_1", "provider_1")
        self.assertEqual(memory, 1024)

    def test_get_architecture(self):
        self.loader._instances_regions_and_providers = {
            "instance_1": {"provider_1": {"config": {"architecture": "x86_64"}}}
        }
        architecture = self.loader.get_architecture("instance_1", "provider_1")
        self.assertEqual(architecture, "x86_64")


if __name__ == "__main__":
    unittest.main()
