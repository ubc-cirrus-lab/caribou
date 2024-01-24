import unittest
from multi_x_serverless.routing.solver_inputs.components.calculators.runtime_calculator import RuntimeCalculator


class TestRuntimeCalculator(unittest.TestCase):
    def test_runtime_calculator(self):
        runtime_calculator = RuntimeCalculator()

        self.assertEqual(runtime_calculator.calculate_execution_time(2.0, 3.0), 6.0)

        transmission_times = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
        self.assertEqual(runtime_calculator.calculate_transmission_latency(transmission_times, 4.0), 6.0)
        self.assertEqual(runtime_calculator.calculate_transmission_latency(transmission_times, 6.0), 6.0)
        self.assertEqual(runtime_calculator.calculate_transmission_latency([], 6.0), -1)


if __name__ == "__main__":
    unittest.main()
