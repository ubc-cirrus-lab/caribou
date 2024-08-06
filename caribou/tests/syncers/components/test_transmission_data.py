import unittest
from datetime import datetime, timedelta
from caribou.syncers.components.transmission_data import TransmissionData


class TestTransmissionData(unittest.TestCase):
    def setUp(self):
        self.taint = "test_taint"
        self.transmission_data = TransmissionData(self.taint)

    def test_init(self):
        self.assertEqual(self.transmission_data.taint, self.taint)
        self.assertIsNone(self.transmission_data.transmission_start_time)
        self.assertIsNone(self.transmission_data.transmission_end_time)
        self.assertEqual(self.transmission_data.payload_transmission_size, 0.0)
        self.assertIsNone(self.transmission_data.from_instance)
        self.assertIsNone(self.transmission_data.to_instance)
        self.assertIsNone(self.transmission_data.from_region)
        self.assertIsNone(self.transmission_data.to_region)
        self.assertIsNone(self.transmission_data.successor_invoked)
        self.assertFalse(self.transmission_data.contains_sync_information)
        self.assertIsNone(self.transmission_data.upload_size)
        self.assertIsNone(self.transmission_data.upload_rtt)
        self.assertIsNone(self.transmission_data.consumed_write_capacity)
        self.assertIsNone(self.transmission_data.sync_data_response_size)
        self.assertIsNone(self.transmission_data.from_direct_successor)
        self.assertIsNone(self.transmission_data.redirector_transmission)
        self.assertIsNone(self.transmission_data.uninvoked_instance)
        self.assertIsNone(self.transmission_data.simulated_sync_predecessor)

    def test_transmission_latency(self):
        self.transmission_data.transmission_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.transmission_data.transmission_end_time = datetime(2024, 1, 1, 11, 0, 0)
        self.assertEqual(self.transmission_data.transmission_latency, 3600.0)

    def test_transmission_latency_not_set(self):
        self.assertIsNone(self.transmission_data.transmission_latency)

        self.transmission_data.transmission_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.assertIsNone(self.transmission_data.transmission_latency)

        self.transmission_data.transmission_start_time = None
        self.transmission_data.transmission_end_time = datetime(2024, 1, 1, 10, 0, 0)
        self.assertIsNone(self.transmission_data.transmission_latency)

    def test_is_completed(self):
        self.transmission_data.from_instance = "instance1"
        self.transmission_data.to_instance = "instance2"
        self.transmission_data.from_region = "region1"
        self.transmission_data.to_region = "region2"
        self.assertTrue(self.transmission_data.is_completed)

    def test_is_not_completed(self):
        self.transmission_data.from_instance = "instance1"
        self.assertFalse(self.transmission_data.is_completed)

        self.transmission_data.to_instance = "instance2"
        self.assertFalse(self.transmission_data.is_completed)

        self.transmission_data.from_region = "region1"
        self.assertFalse(self.transmission_data.is_completed)

        self.transmission_data.to_region = "region2"
        self.assertTrue(self.transmission_data.is_completed)

    def test_to_dict(self):
        self.transmission_data.transmission_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.transmission_data.transmission_end_time = datetime(2024, 1, 1, 11, 0, 0)
        self.transmission_data.payload_transmission_size = 1.0
        self.transmission_data.from_instance = "instance1"
        self.transmission_data.to_instance = "instance2"
        self.transmission_data.from_region = "region1"
        self.transmission_data.to_region = "region2"
        self.transmission_data.successor_invoked = True
        self.transmission_data.from_direct_successor = False
        self.transmission_data.contains_sync_information = True
        self.transmission_data.upload_size = 2.0
        self.transmission_data.consumed_write_capacity = 3.0
        self.transmission_data.sync_data_response_size = 4.0
        self.transmission_data.uninvoked_instance = "instance3"
        self.transmission_data.simulated_sync_predecessor = "instance4"

        expected_dict = {
            "transmission_size_gb": 1.0,
            "transmission_latency_s": 3600.0,
            "from_instance": "instance1",
            "uninvoked_instance": "instance3",
            "simulated_sync_predecessor": "instance4",
            "to_instance": "instance2",
            "from_region": "region1",
            "to_region": "region2",
            "successor_invoked": True,
            "from_direct_successor": False,
            "sync_information": {
                "upload_size_gb": 2.0,
                "consumed_write_capacity": 3.0,
                "sync_data_response_size_gb": 4.0,
            },
        }

        self.assertEqual(self.transmission_data.to_dict(), expected_dict)

    def test_to_dict_no_sync_information(self):
        self.transmission_data.transmission_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.transmission_data.transmission_end_time = datetime(2024, 1, 1, 11, 0, 0)
        self.transmission_data.payload_transmission_size = 1.0
        self.transmission_data.from_instance = "instance1"
        self.transmission_data.to_instance = "instance2"
        self.transmission_data.from_region = "region1"
        self.transmission_data.to_region = "region2"
        self.transmission_data.successor_invoked = True
        self.transmission_data.from_direct_successor = False
        self.transmission_data.uninvoked_instance = "instance3"
        self.transmission_data.simulated_sync_predecessor = "instance4"

        expected_dict = {
            "transmission_size_gb": 1.0,
            "transmission_latency_s": 3600.0,
            "from_instance": "instance1",
            "uninvoked_instance": "instance3",
            "simulated_sync_predecessor": "instance4",
            "to_instance": "instance2",
            "from_region": "region1",
            "to_region": "region2",
            "successor_invoked": True,
            "from_direct_successor": False,
        }

        self.assertEqual(self.transmission_data.to_dict(), expected_dict)

    def test_to_dict_none_values(self):
        self.transmission_data.payload_transmission_size = 1.0

        expected_dict = {"transmission_size_gb": 1.0}

        self.assertEqual(self.transmission_data.to_dict(), expected_dict)

    def test_sync_information_with_none_values(self):
        self.transmission_data.contains_sync_information = True
        self.transmission_data.upload_size = None
        self.transmission_data.consumed_write_capacity = None
        self.transmission_data.sync_data_response_size = None

        expected_dict = {"transmission_size_gb": 0.0, "sync_information": {}}

        self.assertEqual(self.transmission_data.to_dict(), expected_dict)

    def test_transmission_data_full_with_redirector(self):
        # Test with redirector transmission and full data
        self.transmission_data.transmission_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.transmission_data.transmission_end_time = datetime(2024, 1, 1, 10, 0, 30)
        self.transmission_data.payload_transmission_size = 5.0
        self.transmission_data.from_instance = "instance1"
        self.transmission_data.to_instance = "instance2"
        self.transmission_data.from_region = "region1"
        self.transmission_data.to_region = "region2"
        self.transmission_data.successor_invoked = True
        self.transmission_data.from_direct_successor = True
        self.transmission_data.redirector_transmission = True

        expected_dict = {
            "transmission_size_gb": 5.0,
            "transmission_latency_s": 30.0,
            "from_instance": "instance1",
            "to_instance": "instance2",
            "from_region": "region1",
            "to_region": "region2",
            "successor_invoked": True,
            "from_direct_successor": True,
            "redirector_transmission": True,
        }

        self.assertEqual(self.transmission_data.to_dict(), expected_dict)

    def test_to_dict_filters_none_values_correctly(self):
        # Test to_dict when some fields are None
        self.transmission_data.transmission_start_time = datetime(2024, 1, 1, 10, 0, 0)
        self.transmission_data.transmission_end_time = datetime(2024, 1, 1, 10, 0, 10)
        self.transmission_data.from_instance = "instance1"
        self.transmission_data.payload_transmission_size = 10.0
        self.transmission_data.contains_sync_information = True
        self.transmission_data.sync_data_response_size = 10.0

        expected_dict = {
            "transmission_size_gb": 10.0,
            "transmission_latency_s": 10.0,
            "from_instance": "instance1",
            "sync_information": {"sync_data_response_size_gb": 10.0},
        }

        self.assertEqual(self.transmission_data.to_dict(), expected_dict)


if __name__ == "__main__":
    unittest.main()
