import unittest
from unittest.mock import Mock, patch
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig
from multi_x_serverless.routing.current.ranker.ranker import Ranker


class TestRanker(unittest.TestCase):
    def setUp(self):
        self.config = Mock(spec=WorkflowConfig)
        self.ranker = Ranker(self.config)

    def test_rank_without_priority(self):
        self.config.constraints = None
        results = [
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function2"}, 1.0, 2.0, 1.0),
            ({"function": "function3"}, 3.0, 3.0, 2.0),
        ]
        expected_results = [
            ({"function": "function2"}, 1.0, 2.0, 1.0),
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function3"}, 3.0, 3.0, 2.0),
        ]
        self.assertEqual(self.ranker.rank(results), expected_results)

    def test_rank_with_priority_order_no_constraints(self):
        self.config.constraints = {"priority_order": ["runtime", "cost", "carbon"]}
        results = [
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function3"}, 3.0, 2.0, 2.0),
            ({"function": "function2"}, 1.0, 2.0, 1.0),
        ]
        expected_results = [
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function2"}, 1.0, 2.0, 1.0),
            ({"function": "function3"}, 3.0, 2.0, 2.0),
        ]
        self.assertEqual(self.ranker.rank(results), expected_results)

    def test_rank_with_soft_resource_constraints_no_priority_order(self):
        self.config.constraints = {"soft_resource_constraints": {"cost": {"value": 2.0}, "runtime": {"value": 2.0}}}
        results = [
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function2"}, 1.0, 2.0, 1.0),
            ({"function": "function3"}, 3.0, 3.0, 2.0),
        ]
        expected_results = [
            ({"function": "function2"}, 1.0, 2.0, 1.0),
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function3"}, 3.0, 3.0, 2.0),
        ]
        self.assertEqual(self.ranker.rank(results), expected_results)

    def test_rank_with_soft_resource_constraints_with_priority_order(self):
        self.config.constraints = {
            "priority_order": ["runtime", "cost", "carbon"],
            "soft_resource_constraints": {"cost": {"value": 2.0}, "runtime": {"value": 2.0}, "carbon": {"value": 2.0}},
        }
        results = [
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function2"}, 1.0, 2.0, 1.0),
            ({"function": "function3"}, 3.0, 3.0, 2.0),
        ]
        expected_results = [
            ({"function": "function2"}, 1.0, 2.0, 1.0),
            ({"function": "function1"}, 2.0, 1.0, 3.0),
            ({"function": "function3"}, 3.0, 3.0, 2.0),
        ]
        self.assertEqual(self.ranker.rank(results), expected_results)

    def test__get_number_of_violated_constraints(self):
        soft_resource_constraints = {"cost": {"value": 2.0}, "runtime": {"value": 2.0}, "carbon": {"value": 2.0}}
        result = ({"function": "function1"}, 2.0, 3.0, 3.0)
        expected_violated_constraints = 2
        self.assertEqual(
            self.ranker._get_number_of_violated_constraints(soft_resource_constraints, *result[1:]),
            expected_violated_constraints,
        )


if __name__ == "__main__":
    unittest.main()
