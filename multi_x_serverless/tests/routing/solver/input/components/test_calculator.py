import unittest
from multi_x_serverless.routing.solver.input.components.calculator import InputCalculator

class TestInputCalculator(unittest.TestCase):
    def setUp(self):
        self.calculator = InputCalculator()

    def test_setup(self):
        self.calculator.setup()
        self.assertEqual(self.calculator._data_cache, {})

    def test_str(self):
        self.assertEqual(str(self.calculator), "InputCalculator(name=InputCalculator)")

    def test_repr(self):
        self.assertEqual(repr(self.calculator), "InputCalculator(name=InputCalculator)")

if __name__ == '__main__':
    unittest.main()