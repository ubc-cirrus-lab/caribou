import unittest
from unittest.mock import MagicMock, patch
from caribou.deployment_solver.workflow_config import WorkflowConfig
from caribou.deployment_solver.ranker.ranker import Ranker


class TestRanker(unittest.TestCase):
    def test_get_number_one_priority_no_constraints(self):
        # Arrange
        mock_config = MagicMock()
        mock_config.constraints = None
        ranker = Ranker(mock_config)

        # Act
        result = ranker._get_number_one_priority()

        # Assert
        self.assertEqual(result, "average_carbon")

    def test_get_number_one_priority_with_priority_order(self):
        # Arrange
        mock_config = MagicMock()
        mock_config.constraints = {"priority_order": ["runtime", "cost"]}
        ranker = Ranker(mock_config)

        # Act
        result = ranker._get_number_one_priority()

        # Assert
        self.assertEqual(result, "average_runtime")

    @patch.object(Ranker, "_rank_with_soft_resource_constraints")
    @patch.object(Ranker, "_rank_with_priority_order")
    def test_rank_with_soft_resource_constraints(
        self, mock_rank_with_priority_order, mock_rank_with_soft_resource_constraints
    ):
        # Arrange
        mock_config = MagicMock()
        mock_config.constraints = {"soft_resource_constraints": {}}
        ranker = Ranker(mock_config)
        results = [(["region1"], {"average_carbon": 0.1})]

        # Act
        ranker.rank(results)

        # Assert
        mock_rank_with_soft_resource_constraints.assert_called_once_with(results, {})
        mock_rank_with_priority_order.assert_not_called()

    @patch.object(Ranker, "_rank_with_soft_resource_constraints")
    @patch.object(Ranker, "_rank_with_priority_order")
    def test_rank_with_priority_order(self, mock_rank_with_priority_order, mock_rank_with_soft_resource_constraints):
        # Arrange
        mock_config = MagicMock()
        mock_config.constraints = None
        ranker = Ranker(mock_config)
        results = [(["region1"], {"average_carbon": 0.1})]

        # Act
        ranker.rank(results)

        # Assert
        mock_rank_with_priority_order.assert_called_once_with(results)
        mock_rank_with_soft_resource_constraints.assert_not_called()

    def test_rank_with_priority_order_no_constraints(self):
        # Arrange
        mock_config = MagicMock()
        mock_config.constraints = None
        ranker = Ranker(mock_config)
        results = [
            (["region1"], {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3}),
            (["region2"], {"average_carbon": 0.2, "average_runtime": 0.1, "average_cost": 0.3}),
        ]

        # Act
        sorted_results = ranker._rank_with_priority_order(results)

        # Assert
        self.assertEqual(
            sorted_results,
            [
                (["region1"], {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3}),
                (["region2"], {"average_carbon": 0.2, "average_runtime": 0.1, "average_cost": 0.3}),
            ],
        )

    def test_rank_with_priority_order_with_priority_order(self):
        # Arrange
        mock_config = MagicMock()
        mock_config.constraints = {"priority_order": ["runtime", "cost", "carbon"]}
        ranker = Ranker(mock_config)
        results = [
            (["region1"], {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3}),
            (["region2"], {"average_carbon": 0.2, "average_runtime": 0.1, "average_cost": 0.3}),
        ]

        # Act
        sorted_results = ranker._rank_with_priority_order(results)

        # Assert
        self.assertEqual(
            sorted_results,
            [
                (["region2"], {"average_carbon": 0.2, "average_runtime": 0.1, "average_cost": 0.3}),
                (["region1"], {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3}),
            ],
        )

    @patch.object(Ranker, "_get_number_of_violated_constraints")
    @patch.object(Ranker, "_rank_with_priority_order")
    def test_rank_with_soft_resource_constraints(
        self, mock_rank_with_priority_order, mock_get_number_of_violated_constraints
    ):
        # Arrange
        mock_config = MagicMock()
        ranker = Ranker(mock_config)
        results = [
            (["region1"], {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3}),
            (["region2"], {"average_carbon": 0.2, "average_runtime": 0.1, "average_cost": 0.3}),
        ]
        soft_resource_constraints = {}
        mock_get_number_of_violated_constraints.side_effect = [1, 0]
        mock_rank_with_priority_order.side_effect = lambda x: x

        # Act
        ranked_results = ranker._rank_with_soft_resource_constraints(results, soft_resource_constraints)

        # Assert
        self.assertEqual(
            ranked_results,
            [
                (["region2"], {"average_carbon": 0.2, "average_runtime": 0.1, "average_cost": 0.3}),
                (["region1"], {"average_carbon": 0.1, "average_runtime": 0.2, "average_cost": 0.3}),
            ],
        )

    @patch.object(Ranker, "is_absolute_or_relative_failed")
    def test_get_number_of_violated_constraints(self, mock_is_absolute_or_relative_failed):
        # Arrange
        mock_config = MagicMock()
        ranker = Ranker(mock_config)
        ranker._home_deployment_metrics = {
            "average_cost": 0.3,
            "average_runtime": 0.2,
            "average_carbon": 0.1,
        }
        soft_resource_constraints = {
            "cost": 0.4,
            "runtime": 0.3,
            "carbon": 0.2,
        }
        cost = 0.5
        runtime = 0.4
        carbon = 0.3
        mock_is_absolute_or_relative_failed.return_value = True

        # Act
        number_of_violated_constraints = ranker._get_number_of_violated_constraints(
            soft_resource_constraints, cost, runtime, carbon
        )

        # Assert
        self.assertEqual(number_of_violated_constraints, 3)

    def test_is_absolute_or_relative_failed_no_value(self):
        # Arrange
        mock_config = MagicMock()
        ranker = Ranker(mock_config)
        constraint = {"type": "absolute"}

        # Act
        result = ranker.is_absolute_or_relative_failed(0.5, constraint, 0.3)

        # Assert
        self.assertFalse(result)

    def test_is_absolute_or_relative_failed_absolute(self):
        # Arrange
        mock_config = MagicMock()
        ranker = Ranker(mock_config)
        constraint = {"type": "absolute", "value": 0.4}

        # Act
        result = ranker.is_absolute_or_relative_failed(0.5, constraint, 0.3)

        # Assert
        self.assertTrue(result)

    def test_is_absolute_or_relative_failed_relative(self):
        # Arrange
        mock_config = MagicMock()
        ranker = Ranker(mock_config)
        constraint = {"type": "relative", "value": 200}

        # Act
        result = ranker.is_absolute_or_relative_failed(0.5, constraint, 0.3)

        # Assert
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
