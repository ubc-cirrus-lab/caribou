import unittest
from caribou.deployment.common.deploy.models.deployment_plan import DeploymentPlan


class TestDeploymentPlan(unittest.TestCase):
    def test_str(self):
        deployment_plan = DeploymentPlan()
        deployment_plan.instructions = {"aws:us-west-1": ["instruction1", "instruction2"]}
        self.assertEqual(str(deployment_plan), "aws:us-west-1: ['instruction1', 'instruction2']")


if __name__ == "__main__":
    unittest.main()
