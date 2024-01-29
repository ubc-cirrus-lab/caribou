from enum import Enum


class Provider(Enum):
    AWS = "aws"
    GCP = "gcp"

    # For testing purposes
    TEST_PROVIDER1 = "provider1"
    TEST_PROVIDER2 = "provider2"
