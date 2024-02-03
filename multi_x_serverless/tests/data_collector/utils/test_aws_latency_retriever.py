# import unittest
# from unittest.mock import MagicMock, patch
# from multi_x_serverless.data_collector.utils.aws_latency_retriever import AWSLatencyRetriever


# class TestAWSLatencyRetriever(unittest.TestCase):
#     def setUp(self):
#         self.retriever = AWSLatencyRetriever()

#     def test_get_latency(self):
#         # Need to design proper tests to verify the functionalities
#         pass
#         # self.retriever.columns = ['us-east-1', 'us-west-2']
#         # self.retriever.data = {'us-east-1': ['1.0', '2.0'], 'us-west-2': ['3.0', '4.0']}
#         # region_from = {"code": 'us-east-1'}
#         # region_to = {"code": 'us-west-2'}
#         # result = self.retriever.get_latency(region_from, region_to)
#         # self.assertEqual(result, 2.0)

#         # region_from = {"code": 'us-west-2'}
#         # region_to = {"code": 'us-east-1'}
#         # result = self.retriever.get_latency(region_from, region_to)
#         # self.assertEqual(result, 3.0)

#         # region_from = {"code": 'us-west-2'}
#         # region_to = {"code": 'non-existent'}
#         # with self.assertRaises(ValueError):
#         #     self.retriever.get_latency(region_from, region_to)

#         # region_from = {"code": 'non-existent'}
#         # region_to = {"code": 'us-east-1'}
#         # with self.assertRaises(ValueError):
#         #     self.retriever.get_latency(region_from, region_to)


# if __name__ == "__main__":
#     unittest.main()
