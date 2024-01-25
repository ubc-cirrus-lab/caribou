import numpy as np
import unittest
from unittest.mock import patch
from multi_x_serverless.routing.solver_inputs.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager
from multi_x_serverless.routing.solver_inputs.components.runtime_input import RuntimeInput


class TestRuntimeInput(unittest.TestCase):
    @patch.object(DataSourceManager, "get_instance_data")
    @patch.object(DataSourceManager, "get_region_to_region_data")
    @patch.object(DataSourceManager, "get_instance_to_instance_data")
    @patch.object(RuntimeCalculator, "calculate_transmission_latency")
    def test_runtime_input(
        self,
        mock_calculate_transmission_latency,
        mock_get_instance_to_instance_data,
        mock_get_region_to_region_data,
        mock_get_instance_data,
    ):
        mock_get_instance_data.return_value = 1.0
        mock_get_region_to_region_data.return_value = [(1.0, 1.0)]
        mock_get_instance_to_instance_data.return_value = 1.0
        mock_calculate_transmission_latency.return_value = 1.0

        runtime_input = RuntimeInput()
        runtime_input.setup([0], [0], DataSourceManager())

        np.testing.assert_array_equal(runtime_input._execution_matrix, np.array([[1.0]]))
        self.assertEqual(runtime_input._transmission_times_dict, {0: {0: [(1.0, 1.0)]}})
        np.testing.assert_array_equal(runtime_input._data_transfer_size_matrix, np.array([[1.0]]))

        self.assertEqual(runtime_input.get_transmission_value(0, 0, 0, 0, False), 1.0)


if __name__ == "__main__":
    unittest.main()
