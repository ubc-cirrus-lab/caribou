# import unittest
# from unittest.mock import MagicMock, patch
# from multi_x_serverless.data_collector.components.carbon.carbon_transmission_cost_calculator.distance_carbon_transmission_cost_calculator import (
#     DistanceCarbonTransmissionCostCalculator,
# )


# class TestDistanceCarbonTransmissionCostCalculator(unittest.TestCase):
#     def setUp(self):
#         self.get_carbon_intensity_from_coordinates = MagicMock()
#         self.config = {"kwh_per_gb_estimate": 0.1, "_kwh_per_km_gb_estimate": 0.005}
#         self.calculator = DistanceCarbonTransmissionCostCalculator(
#             self.config, self.get_carbon_intensity_from_coordinates
#         )

#     def test_calculate_transmission_carbon_intensity(self):
#         # Need to design proper tests to verify the calculations
#         pass
#         # self.get_carbon_intensity_from_coordinates.return_value = 0.1
#         # region_from = {"latitude": 0, "longitude": 0}
#         # region_to = {"latitude": 1, "longitude": 1}
#         # result = self.calculator.calculate_transmission_carbon_intensity(region_from, region_to)
#         # # Add assertions to check the result
#         # self.assertAlmostEqual(result, 0.1, places=1)

#     def test_calculate_carbon_intensity_segment(self):
#         # Need to design proper tests to verify the calculations
#         pass
#         # result = self.calculator._calculate_carbon_intensity_segment(0, 0)
#         # self.assertEqual(result, 0)

#         # result = self.calculator._calculate_carbon_intensity_segment(1, 1)
#         # # Add assertions to check the result
#         # self.assertAlmostEqual(result, 0.105, places=3)


# if __name__ == "__main__":
#     unittest.main()
