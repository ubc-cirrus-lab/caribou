# import unittest
# from unittest.mock import MagicMock, patch
# from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.carbon_transmission_cost_calculator import (
#     CarbonTransmissionCostCalculator,
# )


# class MockCarbonTransmissionCostCalculator(CarbonTransmissionCostCalculator):
#     def __init__(self, config, get_carbon_intensity_from_coordinates):
#         super().__init__(config, get_carbon_intensity_from_coordinates)

#     def calculate_transmission_carbon_intensity(self, region_from, region_to):
#         pass


# class TestCarbonTransmissionCostCalculator(unittest.TestCase):
#     def setUp(self):
#         self.get_carbon_intensity_from_coordinates = MagicMock()
#         self.config = {"kwh_per_gb_estimate": 0.1}
#         self.calculator = MockCarbonTransmissionCostCalculator(self.config, self.get_carbon_intensity_from_coordinates)

#     def test_get_distance_between_coordinates(self):
#         # Need to design proper tests to verify the calculations
#         pass
#         # result = self.calculator._get_distance_between_coordinates(0, 0, 0, 0)
#         # self.assertEqual(result, 0)

#         # result = self.calculator._get_distance_between_coordinates(0, 0, 1, 1)
#         # # Add assertions to check the result
#         # self.assertAlmostEqual(result, 157.2, places=1)

#     def test_get_carbon_intensity_segments_from_coordinates(self):
#         # Need to design proper tests to verify the calculations
#         pass

#     #     self.get_carbon_intensity_from_coordinates.return_value = 0.1
#     #     result = self.calculator._get_carbon_intensity_segments_from_coordinates(0, 0, 0, 0)
#     #     self.assertEqual(result, [(0, 0.1)])

#     #     self.get_carbon_intensity_from_coordinates.return_value = 0.2
#     #     result = self.calculator._get_carbon_intensity_segments_from_coordinates(0, 0, 1, 1)
#     #     # Add assertions to check the result
#     #     self.assertEqual(result, [(0, 0.2)])


# if __name__ == "__main__":
#     unittest.main()
