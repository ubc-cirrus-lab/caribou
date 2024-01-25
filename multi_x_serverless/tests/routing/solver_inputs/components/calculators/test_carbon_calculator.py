import unittest
from multi_x_serverless.routing.solver_inputs.components.calculators.carbon_calculator import CarbonCalculator


class TestCarbonCalculator(unittest.TestCase):
    def test_carbon_calculator(self):
        carbon_calculator = CarbonCalculator()

        compute_configuration = {"memory": 1.0, "vcpu": 2.0}
        execution_time = 3600.0
        grid_co2e = 0.5
        pue = 1.5
        cfe = 0.1
        average_kw_compute = 0.5
        memory_kw_mb = 0.5
        expected_execution_carbon = (
            (average_kw_compute * compute_configuration["vcpu"] + memory_kw_mb * compute_configuration["memory"])
            * (1 - cfe)
            * pue
            * grid_co2e
        )
        self.assertEqual(
            carbon_calculator.calculate_execution_carbon(
                compute_configuration, execution_time, grid_co2e, pue, cfe, average_kw_compute, memory_kw_mb
            ),
            expected_execution_carbon,
        )

        transmission_co2e_per_gb = 0.5
        data_transfer_size = 2.0
        expected_transmission_carbon = transmission_co2e_per_gb * data_transfer_size
        self.assertEqual(
            carbon_calculator.calculate_transmission_carbon(transmission_co2e_per_gb, data_transfer_size),
            expected_transmission_carbon,
        )


if __name__ == "__main__":
    unittest.main()
