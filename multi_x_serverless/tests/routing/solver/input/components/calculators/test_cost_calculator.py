import unittest
from unittest.mock import Mock
from multi_x_serverless.routing.solver.input.components.calculators.runtime_calculator import RuntimeCalculator
from multi_x_serverless.routing.solver.input.components.loaders.datacenter_loader import DatacenterLoader
from multi_x_serverless.routing.solver.input.components.loaders.workflow_loader import WorkflowLoader
from multi_x_serverless.routing.solver.input.components.calculators.cost_calculator import CostCalculator

class TestCostCalculator(unittest.TestCase):
    def setUp(self):
        self.datacenter_loader = Mock(spec=DatacenterLoader)
        self.workflow_loader = Mock(spec=WorkflowLoader)
        self.runtime_calculator = Mock(spec=RuntimeCalculator)
        self.cost_calculator = CostCalculator(
            self.datacenter_loader,
            self.workflow_loader,
            self.runtime_calculator
        )

    def test_calculate_execution_cost(self):
        self.cost_calculator._calculate_raw_execution_cost = Mock(return_value=10.0)
        result = self.cost_calculator.calculate_execution_cost('instance1', "provider1:region1")
        self.assertEqual(result, 10.0)

    def test_calculate_transmission_cost(self):
        self.cost_calculator._calculate_raw_transmission_cost = Mock(return_value=5.0)
        result = self.cost_calculator.calculate_transmission_cost('instance1', 'instance2', "provider1:region1", "provider1:region2")
        self.assertEqual(result, 5.0)

    def test__calculate_raw_execution_cost(self):
        self.runtime_calculator.calculate_raw_runtime.return_value = 3600
        self.workflow_loader.get_vcpu.return_value = 2
        self.workflow_loader.get_memory.return_value = 4096
        self.workflow_loader.get_architecture.return_value = 'x86'
        self.datacenter_loader.get_compute_cost.return_value = 0.01
        self.datacenter_loader.get_invocation_cost.return_value = 0.00001

        result = self.cost_calculator._calculate_raw_execution_cost('instance1', "provider1:region1")
        self.assertEqual(result, 0.01 * 2 * 4 * 3600 + 0.00001)

    def test__calculate_raw_transmission_cost(self):
        self.workflow_loader.get_data_transfer_size.return_value = 2
        self.datacenter_loader.get_transmission_cost.return_value = 0.01

        result = self.cost_calculator._calculate_raw_transmission_cost('instance1', 'instance2', "provider1:region1", "provider1:region2")
        self.assertEqual(result, 2 * 0.01)

if __name__ == '__main__':
    unittest.main()