import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.data_sources.components.source import Source
from multi_x_serverless.routing.data_sources.components.region_specific.carbon import CarbonSource
from multi_x_serverless.routing.data_sources.components.region_specific.cost import CostSource
from multi_x_serverless.routing.data_sources.components.runtime import RuntimeSource
from multi_x_serverless.routing.data_sources.components.instance_specific.data_transfer import DataTransferSource
from multi_x_serverless.routing.data_sources.data_manager import DataManager

class TestDataManager(unittest.TestCase):
    @patch('multi_x_serverless.routing.models.indexer.Indexer')
    @patch('multi_x_serverless.routing.data_sources.components.carbon.CarbonSource')
    @patch('multi_x_serverless.routing.data_sources.components.cost.CostSource')
    @patch('multi_x_serverless.routing.data_sources.components.runtime.RuntimeSource')
    @patch('multi_x_serverless.routing.data_sources.components.file.FileSource')
    def setUp(self, mock_file_source, mock_runtime_source, mock_cost_source, mock_carbon_source, mock_indexer):
        self.file_source = mock_file_source.return_value
        self.runtime_source = mock_runtime_source.return_value
        self.cost_source = mock_cost_source.return_value
        self.carbon_source = mock_carbon_source.return_value
        self.indexer = mock_indexer.return_value
        self.config = Mock()
        self.data_manager = DataManager(self.config, self.indexer, self.indexer)

        self.data_manager._carbon_source = self.carbon_source
        self.data_manager._cost_source = self.cost_source
        self.data_manager._runtime_source = self.runtime_source
        self.data_manager._file_source = self.file_source

    def test_setup(self):
        regions = Mock()
        self.data_manager.setup(regions)
        self.carbon_source.setup.assert_called_once_with(regions, self.config, self.indexer, self.indexer)
        self.cost_source.setup.assert_called_once_with(regions, self.config, self.indexer, self.indexer)
        self.runtime_source.setup.assert_called_once_with(regions, self.config, self.indexer, self.indexer)
        self.file_source.setup.assert_called_once_with(regions, self.config, self.indexer, self.indexer)

    def test_get_execution_matrix(self):
        self.data_manager.get_execution_matrix("Cost")
        self.cost_source.get_execution_matrix.assert_called_once()

    def test_get_transmission_matrix(self):
        self.data_manager.get_transmission_matrix("Cost")
        self.cost_source.get_transmission_matrix.assert_called_once()

if __name__ == '__main__':
    unittest.main()