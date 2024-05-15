from enum import Enum


# NOTE: Provider name MUST NOT have _ in it.
class Provider(Enum):
    AWS = "aws"
    GCP = "gcp"

    # For testing purposes
    TEST_PROVIDER1 = "provider1"
    TEST_PROVIDER2 = "provider2"

    # For integration tests
    INTEGRATION_TEST_PROVIDER = "IntegrationTestProvider"
