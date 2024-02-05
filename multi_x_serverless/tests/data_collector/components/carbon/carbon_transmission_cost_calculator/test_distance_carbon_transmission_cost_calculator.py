from unittest import TestCase
import unittest
from unittest.mock import MagicMock
from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.distance_carbon_transmission_cost_calculator import (
    DistanceCarbonTransmissionCostCalculator,
)


class TestDistanceCarbonTransmissionCostCalculator(TestCase):
    def setUp(self):
        self.get_carbon_intensity_from_coordinates = MagicMock(return_value=0.5)
        self.calculator = DistanceCarbonTransmissionCostCalculator({}, self.get_carbon_intensity_from_coordinates)

    def test_calculate_transmission_carbon_intensity(self):
        # Mock the _get_carbon_intensity_segments_from_coordinates method
        self.calculator._get_carbon_intensity_segments_from_coordinates = MagicMock(return_value=[(0.0, 0.5)])

        # Call the method and check the result
        result = self.calculator.calculate_transmission_carbon_intensity(
            {"latitude": 0, "longitude": 0}, {"latitude": 0, "longitude": 1}
        )
        self.assertEqual(result, 0.05)

    def test__calculate_carbon_intensity_segment(self):
        # Call the method and check the result
        result = self.calculator._calculate_carbon_intensity_segment(1.0, 0.5)
        self.assertAlmostEqual(result, 0.0525, places=4)


if __name__ == "__main__":
    unittest.main()
