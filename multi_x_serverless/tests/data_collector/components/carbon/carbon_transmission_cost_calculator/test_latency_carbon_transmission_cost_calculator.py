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

    def test_calculate_transmission_carbon_intensity(self):
        # Mock the _get_total_latency and _get_carbon_intensity_segments_from_coordinates methods
        self.calculator._get_total_latency = MagicMock(return_value=1.0)
        self.calculator._get_carbon_intensity_segments_from_coordinates = MagicMock(return_value=[(0.0, 0.5)])
        self.calculator._total_distance = 1.0

        # Call the method and check the result
        result = self.calculator.calculate_transmission_carbon_intensity(
            {"latitude": 0, "longitude": 0}, {"latitude": 0, "longitude": 1}
        )
        self.assertAlmostEqual(result, 0.05, places=10)

    def test__calculate_carbon_intensity_segment(self):
        # Call the method and check the result
        result = self.calculator._calculate_carbon_intensity_segment(1.0, 0.5)
        self.assertAlmostEqual(result, 0.05000833, places=8)

    def test__get_total_latency(self):
        # Mock the _aws_latency_retriever.get_latency method
        self.calculator._aws_latency_retriever.get_latency = MagicMock(return_value=1.0)

        # Call the method and check the result
        result = self.calculator._get_total_latency({"provider": "aws"}, {"provider": "aws"})
        self.assertEqual(result, 1.0)

        result = self.calculator._get_total_latency({"provider": "default"}, {"provider": "aws"})

        self.assertEqual(result, 0.0)


if __name__ == "__main__":
    unittest.main()
