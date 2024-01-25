import numpy as np
import unittest
from unittest.mock import patch
from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager
from multi_x_serverless.routing.solver_inputs.components.runtime_input import RuntimeInput
from multi_x_serverless.routing.solver_inputs.components.cost_input import CostInput


class TestCostInput(unittest.TestCase):
    @patch.object(DataSourceManager, "get_region_data")
    @patch.object(DataSourceManager, "get_instance_data")
    @patch.object(DataSourceManager, "get_region_to_region_data")
    @patch.object(DataSourceManager, "get_instance_to_instance_data")
    @patch.object(RuntimeInput, "get_execution_value")
    def test_cost_input(
        self,
        mock_get_execution_value,
        mock_get_instance_to_instance_data,
        mock_get_region_to_region_data,
        mock_get_instance_data,
        mock_get_region_data,
    ):
        mock_get_region_data.return_value = 1.0
        mock_get_instance_data.return_value = {"provider1": {"value1": 1.0}}
        mock_get_region_to_region_data.return_value = 1.0
        mock_get_instance_to_instance_data.return_value = 1.0
        mock_get_execution_value.return_value = 1.0

        cost_input = CostInput()
        cost_input.setup([0], [0], DataSourceManager(), RuntimeInput())

        np.testing.assert_array_equal(cost_input._execution_matrix, np.array([[0.0]]))
        np.testing.assert_array_equal(cost_input._transmission_cost_matrix, np.array([[2.0]]))
        np.testing.assert_array_equal(cost_input._data_transfer_size_matrix, np.array([[1.0]]))

        self.assertEqual(cost_input.get_transmission_value(0, 0, 0, 0, False), 2.0)


if __name__ == "__main__":
    unittest.main()
