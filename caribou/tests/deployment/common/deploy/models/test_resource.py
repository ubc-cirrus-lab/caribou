import unittest
from caribou.deployment.common.deploy.models.resource import Resource


class TestResource(unittest.TestCase):
    def setUp(self):
        self.resource = Resource("resource_name", "resource_type")

    def test_init(self):
        self.assertEqual(self.resource.name, "resource_name")
        self.assertEqual(self.resource.resource_type, "resource_type")

    def test_dependencies(self):
        self.assertEqual(self.resource.dependencies(), [])

    def test_get_deployment_instructions(self):
        self.assertEqual(self.resource.get_deployment_instructions(), {})

    def test_repr(self):
        self.assertEqual(repr(self.resource), "Resource(name=resource_name, resource_type=resource_type, ")

    def test_eq(self):
        other_resource = Resource("resource_name", "resource_type")
        self.assertEqual(self.resource, other_resource)

        other_resource = Resource("other_name", "other_type")
        self.assertNotEqual(self.resource, other_resource)


if __name__ == "__main__":
    unittest.main()
