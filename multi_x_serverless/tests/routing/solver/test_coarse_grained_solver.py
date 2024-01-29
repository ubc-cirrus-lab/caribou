import unittest
from unittest.mock import Mock, patch
import numpy as np

# from multi_x_serverless.routing.solver.simple_solver import SimpleSolver
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.solver.coarse_grained_solver import CoarseGrainedSolver
from multi_x_serverless.routing.solver_inputs.input_manager import InputManager


class TestCoarseGrainedSolver(unittest.TestCase):
    execution_matrix: list[list[int]]
    transmission_matrix: list[list[int]]

    def setUp(self) -> None:
        # Say this is the value of deploying an instance at a region (row = from, col = to) # row = instance_index, column = region_index
        # For simplicity, we just say that cost just this value, co2 is this value * 2, and rt is also just this value
        self.execution_matrix = [
            [5.0, 6.0],
            [7.0, 8.0],
            [9.0, 10.0],
            [11.0, 12.0],
            [13.0, 14.0],
            [15.0, 16.0],
        ]

        # Simplify it to 1 array as it might be easier to understand (So say cos/co2/rt have same base values)
        # previous_instance_index, current_instance_index, from_region_index, to_region_index
        # Here we only consider the from to regions
        # But we just use a factor and then simply use from to of regions here
        # Say this is the value of from a region to a region (row = from, col = to)
        # For simplicity, we just say that cost just this value, co2 is this value * 2, and rt is also just this value
        self.transmission_matrix = [
            [0, 1],
            [2, 0],
        ]

    @patch.object(CoarseGrainedSolver, "_get_permitted_region_indices")
    @patch.object(CoarseGrainedSolver, "_fail_hard_resource_constraints")
    @patch.object(CoarseGrainedSolver, "init_deployment_to_region")
    def test_solve(
        self, mock_init_deployment_to_region, mock_fail_hard_resource_constraints, mock_get_permitted_region_indices
    ):
        workflow_config = Mock(spec=WorkflowConfig)
        workflow_config.instances = [
            {
                "instance_name": "node1",
                "succeeding_instances": ["node2"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
            {
                "instance_name": "node2",
                "succeeding_instances": [],
                "preceding_instances": ["node1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
        ]
        workflow_config.regions_and_providers = {
            "allowed_regions": None,
            "disallowed_regions": None,
            "providers": {"provider1": None, "provider2": None},
        }

        input_manager = Mock(spec=InputManager)
        input_manager.get_execution_cost_carbon_runtime.side_effect = (
            lambda current_instance_index, to_region_index, consider_probabilistic_invocations=False: (
                (self.execution_matrix[current_instance_index][to_region_index]),
                (self.execution_matrix[current_instance_index][to_region_index] * 2),
                (self.execution_matrix[current_instance_index][to_region_index]),
            )
        )

        input_manager.get_transmission_cost_carbon_runtime.side_effect = (
            lambda previous_instance_index, current_instance_index, from_region_index, to_region_index, consider_probabilistic_invocations=False: (
                (self.transmission_matrix[from_region_index][to_region_index]),
                (self.transmission_matrix[from_region_index][to_region_index] * 2),
                (self.transmission_matrix[from_region_index][to_region_index]),
            )
            if from_region_index is not None and previous_instance_index is not None
            else (0, 0, 0)  # Do not consider start hop
        )
        workflow_config.workflow_id = "workflow_id"
        workflow_config.start_hops = {"provider": "provider1", "region": "region1"}
        # Arrange
        mock_get_permitted_region_indices.return_value = [0, 1]
        mock_fail_hard_resource_constraints.return_value = False
        mock_init_deployment_to_region.return_value = ({0: 0}, (1, 1), (2, 2), (3, 3))
        regions = [{"provider": "provider1", "region": "region1"}, {"provider": "provider2", "region": "region2"}]
        solver = CoarseGrainedSolver(workflow_config, all_available_regions=regions, input_manager=input_manager)

        # Act
        result = solver._solve(regions)

        # Assert
        expected_result = [({0: 0}, 1, 2, 3)]
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
