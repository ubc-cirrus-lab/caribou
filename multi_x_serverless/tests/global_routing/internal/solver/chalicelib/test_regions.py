from unittest.mock import patch
from multi_x_serverless.global_routing.internal.solver.chalicelib.regions import get_regions, filter_regions


@patch("boto3.client")
def test_get_regions(mock_client):
    # Mock the DynamoDB client
    mock_response = {
        "Items": [
            {"region_code": {"S": "us-west-2"}, "provider": {"S": "AWS"}},
            {"region_code": {"S": "eu-west-1"}, "provider": {"S": "Azure"}},
            # Add more mock items as needed
        ]
    }
    mock_client.return_value.scan.return_value = mock_response

    # Call the get_regions function
    regions = get_regions()

    # Assert the expected results
    expected_regions = [("us-west-2", "AWS"), ("eu-west-1", "Azure")]
    assert regions == expected_regions


def test_filter_regions():
    # Create a mock regions array
    regions = [("us-west-2", "AWS"), ("eu-west-1", "Azure")]

    workflow_description = {"filtered_regions": ["Azure:eu-west-1"]}

    # Call the filter_regions function
    filtered_regions = filter_regions(regions, workflow_description)

    # Assert the expected results
    expected_filtered_regions = [("us-west-2", "AWS")]
    assert filtered_regions == expected_filtered_regions
