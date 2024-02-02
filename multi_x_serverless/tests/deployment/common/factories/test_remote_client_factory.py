import unittest
from multi_x_serverless.common.models.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.deployment.common.factories.remote_client_factory import RemoteClientFactory


class TestRemoteClientFactory(unittest.TestCase):
    def test_get_remote_client_aws(self):
        # Arrange
        provider = "aws"
        region = "region1"

        # Act
        remote_client = RemoteClientFactory.get_remote_client(provider, region)

        # Assert
        self.assertIsInstance(remote_client, AWSRemoteClient)

    def test_get_remote_client_unknown(self):
        # Arrange
        provider = "unknown"
        region = "region1"

        # Act & Assert
        with self.assertRaises(RuntimeError):
            RemoteClientFactory.get_remote_client(provider, region)

    def test_get_remote_client_gcp(self):
        # Arrange
        provider = "gcp"
        region = "region1"

        # Act & Assert
        with self.assertRaises(NotImplementedError):
            RemoteClientFactory.get_remote_client(provider, region)


if __name__ == "__main__":
    unittest.main()
