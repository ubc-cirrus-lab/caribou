import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.at.instance_source import InstanceSource


class TestInstanceSource(unittest.TestCase):
    def test_instance_source(self):
        instance_source = InstanceSource()

        mock_indexer = Mock(spec=Indexer)
        mock_indexer.value_to_index.return_value = 0

        loaded_data = {"execution_time": {"instance1": 1.0}}
        instance_configuration = [
            {
                "instance_name": "instance1",
                "regions_and_providers": {"providers": {"provider1": {"memory": 1769, "vcpu": 1.0}}},
            }
        ]
        instances = ["instance1"]

        instance_source.setup(loaded_data, instances, mock_indexer, configurations=instance_configuration)

        self.assertEqual(
            instance_source._data,
            {0: {"execution_time": 1.0, "provider_configurations": {"provider1": {"memory": 1769, "vcpu": 1.0}}}},
        )

        self.assertEqual(instance_source.get_value("execution_time", 0), 1.0)
        self.assertEqual(
            instance_source.get_value("provider_configurations", 0), {"provider1": {"memory": 1769, "vcpu": 1.0}}
        )


if __name__ == "__main__":
    unittest.main()
