from unittest import TestCase
import unittest
from unittest.mock import MagicMock
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.latency_carbon_transmission_cost_calculator import (
    LatencyCarbonTransmissionCostCalculator,
)


class TestLatencyCarbonTransmissionCostCalculator(TestCase):
    def setUp(self):
        self.get_carbon_intensity_from_coordinates = MagicMock(return_value=0.5)
        self.calculator = LatencyCarbonTransmissionCostCalculator({}, self.get_carbon_intensity_from_coordinates)


if __name__ == "__main__":
    unittest.main()
