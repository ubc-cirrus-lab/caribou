import unittest
from unittest.mock import Mock
from caribou.syncers.components.execution_data import ExecutionData
from caribou.syncers.components.start_hop_data import StartHopData


class TestStartHopData(unittest.TestCase):
    def setUp(self) -> None:
        self.start_hop_data = StartHopData()

    def test_init(self) -> None:
        """Test the initial state of StartHopData."""
        self.assertIsNone(self.start_hop_data.request_source)
        self.assertIsNone(self.start_hop_data.destination_provider_region)
        self.assertIsNone(self.start_hop_data.user_payload_size)
        self.assertIsNone(self.start_hop_data.wpd_data_size)
        self.assertIsNone(self.start_hop_data.consumed_read_capacity)
        self.assertIsNone(self.start_hop_data.init_latency_from_first_recieved)
        self.assertIsNone(self.start_hop_data.time_from_function_start_to_entry_point)
        self.assertIsNone(self.start_hop_data.start_hop_latency_from_client)
        self.assertIsNone(self.start_hop_data.overridden_wpd_data_size)
        self.assertFalse(self.start_hop_data.retrieved_wpd_at_function)
        self.assertIsNone(self.start_hop_data.redirector_execution_data)
        self.assertEqual(self.start_hop_data.encountered_request_ids, set())

    def test_get_redirector_execution_data_no_existing_data(self) -> None:
        """Test getting redirector execution data when none exists."""
        execution_data = self.start_hop_data.get_redirector_execution_data("test_instance", "request_id_1")
        self.assertIsInstance(execution_data, ExecutionData)
        self.assertEqual(execution_data.instance_name, "test_instance")
        self.assertEqual(execution_data.request_id, "request_id_1")
        self.assertIn("request_id_1", self.start_hop_data.encountered_request_ids)

    def test_get_redirector_execution_data_existing_data(self) -> None:
        """Test getting redirector execution data when it already exists."""
        execution_data_1 = self.start_hop_data.get_redirector_execution_data("test_instance", "request_id_1")
        execution_data_2 = self.start_hop_data.get_redirector_execution_data("test_instance", "request_id_2")

        self.assertIs(execution_data_1, execution_data_2)
        self.assertEqual(execution_data_1.request_id, "request_id_1")  # Request ID shouldn't change
        self.assertIn("request_id_1", self.start_hop_data.encountered_request_ids)
        self.assertIn("request_id_2", self.start_hop_data.encountered_request_ids)

    def test_get_redirector_execution_data_mismatched_instance_name(self) -> None:
        """Test error when mismatched instance names are provided."""
        self.start_hop_data.get_redirector_execution_data("test_instance", "request_id_1")

        with self.assertRaises(ValueError):
            self.start_hop_data.get_redirector_execution_data("different_instance", "request_id_2")

    def test_is_completed_without_redirector(self) -> None:
        """Test completion status without redirector data."""
        self.start_hop_data.request_source = "source"
        self.start_hop_data.destination_provider_region = "destination"
        self.start_hop_data.user_payload_size = 1.0
        self.start_hop_data.wpd_data_size = 1.0
        self.start_hop_data.consumed_read_capacity = 1.0
        self.start_hop_data.time_from_function_start_to_entry_point = 1.0
        self.start_hop_data.start_hop_latency_from_client = 1.0

        self.assertTrue(self.start_hop_data.is_completed)

    def test_is_not_completed_missing_required_fields(self) -> None:
        """Test incomplete status due to missing required fields."""
        self.assertFalse(self.start_hop_data.is_completed)

        self.start_hop_data.request_source = "source"
        self.assertFalse(self.start_hop_data.is_completed)

        self.start_hop_data.destination_provider_region = "destination"
        self.assertFalse(self.start_hop_data.is_completed)

        self.start_hop_data.user_payload_size = 1.0
        self.assertFalse(self.start_hop_data.is_completed)

        self.start_hop_data.wpd_data_size = 1.0
        self.assertFalse(self.start_hop_data.is_completed)

        self.start_hop_data.consumed_read_capacity = 1.0
        self.assertFalse(self.start_hop_data.is_completed)

        self.start_hop_data.time_from_function_start_to_entry_point = 1.0
        self.assertFalse(self.start_hop_data.is_completed)

        self.start_hop_data.start_hop_latency_from_client = 1.0
        self.assertTrue(self.start_hop_data.is_completed)

    def test_is_not_completed_redirector_data_with_multiple_requests(self) -> None:
        """Test incomplete status when there are multiple request IDs with a redirector."""
        self.start_hop_data.request_source = "source"
        self.start_hop_data.destination_provider_region = "destination"
        self.start_hop_data.user_payload_size = 1.0
        self.start_hop_data.wpd_data_size = 1.0
        self.start_hop_data.consumed_read_capacity = 1.0
        self.start_hop_data.time_from_function_start_to_entry_point = 1.0
        self.start_hop_data.start_hop_latency_from_client = 1.0

        self.assertTrue(self.start_hop_data.is_completed)

        # Add redirector data (incomplete data)
        self.start_hop_data.get_redirector_execution_data("test_instance", "request_id_2")
        self.assertFalse(self.start_hop_data.is_completed)

    def test_to_dict_no_redirector(self) -> None:
        """Test the dictionary output without redirector data."""
        self.start_hop_data.request_source = "source"
        self.start_hop_data.destination_provider_region = "destination"
        self.start_hop_data.user_payload_size = 1.0
        self.start_hop_data.wpd_data_size = 1.0
        self.start_hop_data.consumed_read_capacity = 1.0
        self.start_hop_data.time_from_function_start_to_entry_point = 1.0
        self.start_hop_data.start_hop_latency_from_client = 1.0
        self.start_hop_data.overridden_wpd_data_size = 0.5
        self.start_hop_data.retrieved_wpd_at_function = True

        expected_dict = {
            "destination": "destination",
            "request_source": "source",
            "data_transfer_size_gb": 1.0,
            "latency_from_client_s": 1.0,
            "time_from_function_start_to_entry_point_s": 1.0,
            "workflow_placement_decision": {
                "data_size_gb": 1.0,
                "overridden_data_size_gb": 0.5,
                "consumed_read_capacity": 1.0,
                "retrieved_wpd_at_function": True,
            },
        }

        self.assertEqual(self.start_hop_data.to_dict(), expected_dict)

    def test_to_dict_with_redirector(self) -> None:
        """Test the dictionary output with redirector data."""
        self.start_hop_data.request_source = "source"
        self.start_hop_data.destination_provider_region = "destination"
        self.start_hop_data.user_payload_size = 1.0
        self.start_hop_data.wpd_data_size = 1.0
        self.start_hop_data.consumed_read_capacity = 1.0
        self.start_hop_data.time_from_function_start_to_entry_point = 1.0
        self.start_hop_data.start_hop_latency_from_client = 1.0

        # Mock the redirector execution data
        mock_execution_data = Mock(spec=ExecutionData)
        mock_execution_data.to_dict.return_value = {"execution_info": "value"}
        self.start_hop_data.redirector_execution_data = mock_execution_data

        expected_dict = {
            "destination": "destination",
            "request_source": "source",
            "data_transfer_size_gb": 1.0,
            "latency_from_client_s": 1.0,
            "time_from_function_start_to_entry_point_s": 1.0,
            "workflow_placement_decision": {
                "data_size_gb": 1.0,
                "consumed_read_capacity": 1.0,
                "retrieved_wpd_at_function": False,
            },
            "redirector_execution_data": {"execution_info": "value"},
        }

        self.assertEqual(self.start_hop_data.to_dict(), expected_dict)

    def test_to_dict_excludes_none_fields(self) -> None:
        """Test the dictionary output excludes fields that are None."""
        self.start_hop_data.request_source = "source"
        self.start_hop_data.user_payload_size = 1.0

        expected_dict = {
            "request_source": "source",
            "data_transfer_size_gb": 1.0,
            "workflow_placement_decision": {"retrieved_wpd_at_function": False},
        }

        self.assertEqual(self.start_hop_data.to_dict(), expected_dict)


if __name__ == "__main__":
    unittest.main()
