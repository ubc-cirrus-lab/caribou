from unittest import TestCase
import unittest
from unittest.mock import MagicMock, call
from multi_x_serverless.routing.deployment_input.components.calculators.helpers.carbon_transmission_cost_calculator import (
    CarbonTransmissionCostCalculator,
)


class CarbonTransmissionCostCalculatorInstance(CarbonTransmissionCostCalculator):
    def calculate_transmission_carbon_intensity(self, region_from, region_to):
        pass


class TestCarbonTransmissionCostCalculator(TestCase):
    def setUp(self):
        self.get_carbon_intensity_from_coordinates = MagicMock(return_value=0.5)
        self.calculator = CarbonTransmissionCostCalculatorInstance(self.get_carbon_intensity_from_coordinates)

    def test_get_distance_between_coordinates(self):
        result = self.calculator._get_distance_between_coordinates(0, 0, 0, 1)
        self.assertAlmostEqual(result, 111.19, places=2)

    def test_get_carbon_intensity_segments_from_coordinates(self):
        result = self.calculator._get_carbon_intensity_segments_from_coordinates(0, 0, 0, 1, 100)
        self.assertEqual(result[0][0], 100)
        self.assertAlmostEqual(result[0][1], 0.5, places=2)

    def test_get_carbon_intensity_segments_from_coordinates_complex(self):
        self.calculator._get_carbon_intensity_from_coordinates = MagicMock(side_effect=[0.5, 0.6, 0.7, 0.7, 0.7])
        result = self.calculator._get_carbon_intensity_segments_from_coordinates(0, 0, 0, 6, 600)
        self.assertEqual(result, [(500, 0.5), (100, 0.7)])


if __name__ == "__main__":
    unittest.main()
