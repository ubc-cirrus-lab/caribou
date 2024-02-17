import unittest
from unittest.mock import patch, MagicMock
from multi_x_serverless.data_collector.components.provider.provider_retriever import ProviderRetriever
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
import requests
import os


class TestProviderRetriever(unittest.TestCase):
    def setUp(self):
        self.remote_client = MagicMock(spec=RemoteClient)

        with patch("os.environ.get") as mock_os_environ_get, patch("boto3.client") as mock_boto3, patch(
            "multi_x_serverless.common.utils.str_to_bool"
        ) as mock_str_to_bool:
            mock_boto3.return_value = MagicMock()
            mock_os_environ_get.return_value = "test_key"
            mock_str_to_bool.return_value = False
            self.provider_retriever = ProviderRetriever(self.remote_client)

    @patch("googlemaps.Client")
    def test_retrieve_location(self, mock_googlemaps_client):
        mock_googlemaps_client.return_value.geocode.return_value = [
            {"geometry": {"location": {"lat": 40.7128, "lng": 74.0060}}}
        ]
        lat, lng = self.provider_retriever.retrieve_location("New York")
        self.assertEqual((lat, lng), (40.7128, 74.0060))

    @patch("requests.get")
    @patch("googlemaps.Client")
    def test_retrieve_aws_regions(self, mock_googlemaps_client, mock_requests_get):
        mock_html_content = """
        <html>
            <body>
                <h3>Available Regions</h3>
                <table>
                    <tr><td>us-east-1</td><td>US East (N. Virginia)</td><td>Some other data</td></tr>
                    <tr><td>eu-west-1</td><td>EU (Ireland)</td><td>Some other data</td></tr>
                </table>
            </body>
        </html>
        """
        mock_response = MagicMock()
        mock_response.content = mock_html_content
        mock_requests_get.return_value = mock_response

        mock_googlemaps_client.return_value.geocode.return_value = [
            {"geometry": {"location": {"lat": 37.7749, "lng": -122.4194}}}
        ]


        with patch("os.environ.get") as mock_os_environ_get, patch("boto3.client") as mock_boto3, patch(
            "multi_x_serverless.common.utils.str_to_bool"
        ) as mock_str_to_bool:
            mock_boto3.return_value = MagicMock()
            mock_os_environ_get.return_value = "test_key"
            mock_str_to_bool.return_value = False
            provider_retriever = ProviderRetriever(None)  # Assuming None can be passed as a dummy RemoteClient

        regions = provider_retriever.retrieve_aws_regions()

        self.assertIn("aws:us-east-1", regions)
        self.assertIn("aws:eu-west-1", regions)
        self.assertEqual(regions["aws:us-east-1"]["name"], "US East (N. Virginia)")
        self.assertEqual(regions["aws:us-east-1"]["latitude"], 37.7749)
        self.assertEqual(regions["aws:us-east-1"]["longitude"], -122.4194)

    @patch("requests.get")
    @patch("bs4.BeautifulSoup")
    def test_retrieve_aws_regions_invalid_html(self, mock_beautiful_soup, mock_requests_get):
        mock_response = MagicMock()
        mock_response.content = "<html></html>"  # Simplified HTML content
        mock_requests_get.return_value = mock_response

        mock_soup_instance = MagicMock()
        mock_beautiful_soup.return_value = mock_soup_instance
        mock_soup_instance.find_all.return_value = []  # No tables found scenario

        with self.assertRaises(ValueError) as context:
            self.provider_retriever.retrieve_aws_regions()
        self.assertTrue("Could not find any tables on the AWS regions page" in str(context.exception))

    @patch(
        "multi_x_serverless.data_collector.components.provider.provider_retriever.ProviderRetriever.retrieve_aws_regions"
    )
    def test_retrieve_available_regions(self, mock_retrieve_aws_regions):
        mock_retrieve_aws_regions.return_value = {"aws:dummy_region": {"code": "dummy_region"}}

        result = self.provider_retriever.retrieve_available_regions()
        self.assertIn("aws:dummy_region", result)
        self.assertEqual(result["aws:dummy_region"]["code"], "dummy_region")

    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.boto3.client")
    @patch("requests.get")
    def test_retrieve_aws_execution_cost(self, mock_requests_get, mock_boto3_client):
        mock_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_pricing_client
        mock_pricing_client.list_price_lists.return_value = {"PriceLists": []}

        self.provider_retriever._aws_pricing_client = mock_pricing_client

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_requests_get.return_value = mock_response

        with self.assertRaises(ValueError) as context:
            self.provider_retriever._retrieve_aws_execution_cost(["aws:dummy_region"])
        self.assertTrue("Not all regions have execution cost data" in str(context.exception))

    @patch.dict(os.environ, {"GOOGLE_API_KEY": "fake_key"})
    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.googlemaps.Client")
    def test_retrieve_location_google_maps_failure(self, mock_googlemaps_client):
        mock_googlemaps_client.return_value.geocode.return_value = []

        with self.assertRaises(ValueError) as context:
            self.provider_retriever.retrieve_location("Atlantis")
        self.assertTrue("Could not find location Atlantis" in str(context.exception))

    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.requests.get")
    def test_retrieve_aws_regions_http_error(self, mock_requests_get):
        mock_requests_get.side_effect = requests.exceptions.HTTPError("HTTP Error occurred")

        with self.assertRaises(requests.exceptions.HTTPError):
            self.provider_retriever.retrieve_aws_regions()

    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.str_to_bool")
    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.os.environ.get")
    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.boto3.client")
    def test_aws_pricing_client_initialization_failure(self, mock_boto3_client, mock_os_environ_get, mock_str_to_bool):
        mock_os_environ_get.return_value = None  # No AWS credentials
        mock_str_to_bool.return_value = True  # Is integration test
        mock_boto3_client.side_effect = Exception("AWS client initialization failed")

        with self.assertRaises(Exception) as context:
            self.provider_retriever.__init__(self.remote_client)
        self.assertTrue("AWS client initialization failed" in str(context.exception))

    @patch(
        "multi_x_serverless.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_transmission_cost"
    )
    @patch(
        "multi_x_serverless.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_execution_cost"
    )
    def test_retrieve_provider_region_data_aws(
        self, mock_retrieve_aws_execution_cost, mock_retrieve_aws_transmission_cost
    ):
        mock_retrieve_aws_execution_cost.return_value = {
            "aws:dummy_region": {
                "compute_cost": {"arm64": 0.0001, "x86": 0.00006},
                "invocation_cost": {"arm64": 0.002, "x86_64": 0.003},
            }
        }
        mock_retrieve_aws_transmission_cost.return_value = {"aws:dummy_region": {"global_data_transfer": 0.002}}

        self.provider_retriever._available_regions = {"aws:dummy_region": {"provider": "aws"}}

        result = self.provider_retriever.retrieve_provider_region_data()
        self.assertIn("aws:dummy_region", result)
        self.assertEqual(
            result["aws:dummy_region"]["execution_cost"],
            {"compute_cost": {"arm64": 0.0001, "x86": 0.00006}, "invocation_cost": {"arm64": 0.002, "x86_64": 0.003}},
        )
        self.assertEqual(result["aws:dummy_region"]["transmission_cost"], {"global_data_transfer": 0.002})

    def test_retrieve_aws_transmission_cost(self):
        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:us-west-1"])
        self.assertEqual(
            result, {"aws:us-west-1": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"}}
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:af-south-1"])
        self.assertEqual(
            result,
            {"aws:af-south-1": {"global_data_transfer": 0.154, "provider_data_transfer": 0.147, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-east-1"])
        self.assertEqual(
            result, {"aws:ap-east-1": {"global_data_transfer": 0.12, "provider_data_transfer": 0.09, "unit": "USD/GB"}}
        )

        with self.assertRaises(ValueError):
            self.provider_retriever._retrieve_aws_transmission_cost(["aws:unknown-region"])

        with self.assertRaises(ValueError, msg="Invalid region key us-west-1"):
            self.provider_retriever._retrieve_aws_transmission_cost(["us-west-1"])

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-south-2"])
        self.assertEqual(
            result,
            {"aws:ap-south-2": {"global_data_transfer": 0.1093, "provider_data_transfer": 0.086, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-3"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-3": {"global_data_transfer": 0.132, "provider_data_transfer": 0.10, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-4"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-4": {"global_data_transfer": 0.114, "provider_data_transfer": 0.10, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-south-1"])
        self.assertEqual(
            result,
            {"aws:ap-south-1": {"global_data_transfer": 0.1093, "provider_data_transfer": 0.086, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-northeast-3"])
        self.assertEqual(
            result,
            {"aws:ap-northeast-3": {"global_data_transfer": 0.114, "provider_data_transfer": 0.09, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-northeast-2"])
        self.assertEqual(
            result,
            {"aws:ap-northeast-2": {"global_data_transfer": 0.126, "provider_data_transfer": 0.08, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-1"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-1": {"global_data_transfer": 0.12, "provider_data_transfer": 0.09, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-2"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-2": {"global_data_transfer": 0.114, "provider_data_transfer": 0.098, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-northeast-1"])
        self.assertEqual(
            result,
            {"aws:ap-northeast-1": {"global_data_transfer": 0.114, "provider_data_transfer": 0.09, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ca-central-1"])
        self.assertEqual(
            result,
            {"aws:ca-central-1": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:eu-west-1"])
        self.assertEqual(
            result, {"aws:eu-west-1": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"}}
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:il-central-1"])
        self.assertEqual(
            result,
            {"aws:il-central-1": {"global_data_transfer": 0.11, "provider_data_transfer": 0.08, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:me-south-1"])
        self.assertEqual(
            result,
            {"aws:me-south-1": {"global_data_transfer": 0.117, "provider_data_transfer": 0.1105, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:me-central-1"])
        self.assertEqual(
            result,
            {"aws:me-central-1": {"global_data_transfer": 0.11, "provider_data_transfer": 0.085, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:sa-east-1"])
        self.assertEqual(
            result, {"aws:sa-east-1": {"global_data_transfer": 0.15, "provider_data_transfer": 0.138, "unit": "USD/GB"}}
        )

    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.requests.get")
    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.boto3.client")
    def test_retrieve_aws_execution_cost_success(self, mock_boto3_client, mock_requests_get):
        mock_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_pricing_client

        mock_pricing_client.list_price_lists.return_value = {
            "PriceLists": [
                {"RegionCode": "dummy_region", "PriceListArn": "arn:aws:pricing:::product/aws-lambda/dummy_region"}
            ]
        }

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "terms": {
                "OnDemand": {
                    "invocation_call_sku_arm64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {"pricePerUnit": {"USD": "0.0000166667"}, "endRange": "Inf"}
                            }
                        }
                    },
                    "invocation_duration_sku_arm64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {
                                    "pricePerUnit": {"USD": "0.0000133333"},
                                    "beginRange": "0",
                                    "endRange": "Inf",
                                }
                            }
                        }
                    },
                    "invocation_call_sku_x86_64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {"pricePerUnit": {"USD": "0.0000166667"}, "endRange": "Inf"}
                            }
                        }
                    },
                    "invocation_duration_sku_x86_64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {
                                    "pricePerUnit": {"USD": "0.0000133333"},
                                    "beginRange": "0",
                                    "endRange": "Inf",
                                }
                            }
                        }
                    },
                    "invocation_call_sku_arm64_any": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {"pricePerUnit": {"USD": "0.0000166667"}, "endRange": "5000"}
                            }
                        }
                    },
                    "invocation_duration_sku_arm64_any": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {
                                    "pricePerUnit": {"USD": "0.0000133333"},
                                    "beginRange": "0",
                                    "endRange": "6000",
                                }
                            }
                        }
                    },
                }
            },
            "products": {
                "invocation_call_sku_arm64": {
                    "attributes": {"group": "AWS-Lambda-Requests-ARM", "location": "US East (N. Virginia)"},
                    "sku": "invocation_call_sku_arm64",
                },
                "invocation_duration_sku_arm64": {
                    "attributes": {"group": "AWS-Lambda-Duration-ARM", "location": "US East (N. Virginia)"},
                    "sku": "invocation_duration_sku_arm64",
                },
                "invocation_call_sku_x86_64": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "US East (N. Virginia)"},
                    "sku": "invocation_call_sku_x86_64",
                },
                "invocation_duration_sku_x86_64": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "US East (N. Virginia)"},
                    "sku": "invocation_duration_sku_x86_64",
                },
                "invocation_call_sku_arm64_any": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "Any"},
                    "sku": "invocation_call_sku_arm64_any",
                },
                "invocation_duration_sku_arm64_any": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "Any"},
                    "sku": "invocation_duration_sku_arm64_any",
                },
            },
        }

        mock_requests_get.return_value = mock_response

        self.provider_retriever._aws_pricing_client = mock_pricing_client

        result = self.provider_retriever._retrieve_aws_execution_cost(["aws:dummy_region:dummy_code"])

        self.assertIn("aws:dummy_region:dummy_code", result)
        self.assertIn("invocation_cost", result["aws:dummy_region:dummy_code"])
        self.assertIn("compute_cost", result["aws:dummy_region:dummy_code"])

    @patch("multi_x_serverless.data_collector.components.provider.provider_retriever.boto3.client")
    def test_retrieve_aws_execution_cost_api_failure(self, mock_boto3_client):
        mock_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_pricing_client
        mock_pricing_client.list_price_lists.side_effect = Exception("AWS Pricing API error")

        self.provider_retriever._aws_pricing_client = mock_pricing_client

        with self.assertRaises(Exception) as context:
            self.provider_retriever._retrieve_aws_execution_cost(["aws:dummy_region:dummy_code"])
        self.assertTrue("AWS Pricing API error" in str(context.exception))

    def test_get_aws_product_skus_all_present(self):
        price_list_json = {
            "products": {
                "sku1": {
                    "attributes": {"group": "AWS-Lambda-Requests-ARM", "location": "US East (N. Virginia)"},
                    "sku": "sku1",
                },
                "sku2": {
                    "attributes": {"group": "AWS-Lambda-Duration-ARM", "location": "US East (N. Virginia)"},
                    "sku": "sku2",
                },
                "sku3": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "US East (N. Virginia)"},
                    "sku": "sku3",
                },
                "sku4": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "US East (N. Virginia)"},
                    "sku": "sku4",
                },
                "sku5": {"attributes": {"group": "AWS-Lambda-Requests", "location": "Any"}, "sku": "sku5"},
                "sku6": {"attributes": {"group": "AWS-Lambda-Duration", "location": "Any"}, "sku": "sku6"},
            }
        }

        result = self.provider_retriever.get_aws_product_skus(price_list_json)
        self.assertEqual(result, ("sku1", "sku2", "sku3", "sku4", "sku5", "sku6"))

    def test_get_aws_product_skus_missing_skus(self):
        price_list_json = {
            "products": {
                "sku3": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "US East (N. Virginia)"},
                    "sku": "sku3",
                },
                "sku4": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "US East (N. Virginia)"},
                    "sku": "sku4",
                },
            }
        }

        result = self.provider_retriever.get_aws_product_skus(price_list_json)
        self.assertEqual(result, ("", "", "sku3", "sku4", "", ""))

    def test_get_aws_product_skus_empty_json(self):
        price_list_json = {"products": {}}

        result = self.provider_retriever.get_aws_product_skus(price_list_json)
        self.assertEqual(result, ("", "", "", "", "", ""))


if __name__ == "__main__":
    unittest.main()
