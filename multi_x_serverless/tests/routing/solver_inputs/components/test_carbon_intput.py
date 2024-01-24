import numpy as np
import unittest
from unittest.mock import patch
from multi_x_serverless.routing.solver_inputs.components.data_sources.data_source_manager import DataSourceManager
from multi_x_serverless.routing.solver_inputs.components.runtime_input import RuntimeInput
from multi_x_serverless.routing.solver_inputs.components.carbon_input import CarbonInput


class TestCarbonInput(unittest.TestCase):
    @patch.object(DataSourceManager, "get_region_data")
    @patch.object(DataSourceManager, "get_instance_data")
    @patch.object(DataSourceManager, "get_region_to_region_data")
    @patch.object(DataSourceManager, "get_instance_to_instance_data")
    @patch.object(RuntimeInput, "get_execution_value")
    def test_carbon_input(
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

        carbon_input = CarbonInput()
        carbon_input.setup([0], [0], DataSourceManager(), RuntimeInput())

        np.testing.assert_array_equal(carbon_input._execution_matrix, np.array([[0.0]]))
        np.testing.assert_array_equal(carbon_input._data_transfer_co2e_matrix, np.array([[1.0]]))
        np.testing.assert_array_equal(carbon_input._data_transfer_size_matrix, np.array([[1.0]]))

        self.assertEqual(carbon_input.get_transmission_value(0, 0, 0, 0, False), 1.0)


if __name__ == "__main__":
    unittest.main()
