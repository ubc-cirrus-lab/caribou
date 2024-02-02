# Input format of workflow summarization
# def _mock_workflow_summarized_logs(
#     self,
# ) -> dict[str, Any]:  # Mocked data for testing -> Move to testing when created
#     return {
#         "2021-2-10T10:10:10": {
#             "months_between_summary": 8,
#             "instance_summary": {
#                 "instance_1": {
#                     "invocation_count": 100,
#                     "execution_summary": {
#                         "provider_1:region_1": {
#                             "invocation_count": 90,
#                             "average_runtime": 20,  # In s
#                             "tail_runtime": 30,  # In s
#                         },
#                         "provider_1:region_2": {
#                             "invocation_count": 10,
#                             "average_runtime": 17,  # In s
#                             "tail_runtime": 25,  # In s
#                         },
#                     },
#                     "invocation_summary": {
#                         "instance_2": {
#                             "invocation_count": 80,
#                             "average_data_transfer_size": 0.0007,  # In GB
#                             "transmission_summary": {
#                                 "provider_1:region_1": {
#                                     "provider_1:region_1": {
#                                         "transmission_count": 50,
#                                         "average_latency": 0.001,  # In s
#                                         "tail_latency": 0.002,  # In s
#                                     },
#                                     "provider_1:region_2": {
#                                         "transmission_count": 22,
#                                         "average_latency": 0.12,  # In s
#                                         "tail_latency": 0.15,  # In s
#                                     },
#                                 },
#                                 "provider_1:region_2": {
#                                     "provider_1:region_1": {
#                                         "transmission_count": 8,
#                                         "average_latency": 0.1,  # In s
#                                         "tail_latency": 0.12,  # In s
#                                     }
#                                 },
#                             },
#                         }
#                     },
#                 },
#                 "instance_2": {
#                     "invocation_count": 80,
#                     "execution_summary": {
#                         "provider_1:region_1": {
#                             "invocation_count": 58,
#                             "average_runtime": 10,  # In s
#                             "tail_runtime": 15,  # In s
#                         },
#                         "provider_1:region_2": {
#                             "invocation_count": 22,
#                             "average_runtime": 12,  # In s
#                             "tail_runtime": 17,  # In s
#                         },
#                     },
#                 },
#             },
#         },
#         "2021-10-10T10:10:20": {
#             "months_between_summary": 8,
#             "instance_summary": {
#                 "instance_1": {
#                     "invocation_count": 200,
#                     "execution_summary": {
#                         "provider_1:region_1": {
#                             "invocation_count": 144,
#                             "average_runtime": 22,  # In s
#                             "tail_runtime": 33,  # In s
#                         },
#                         "provider_1:region_2": {
#                             "invocation_count": 56,
#                             "average_runtime": 15,  # In s
#                             "tail_runtime": 27,  # In s
#                         },
#                     },
#                     "invocation_summary": {
#                         "instance_2": {
#                             "invocation_count": 160,
#                             "average_data_transfer_size": 0.0007,  # In GB
#                             "transmission_summary": {
#                                 "provider_1:region_1": {
#                                     "provider_1:region_1": {
#                                         "transmission_count": 100,
#                                         "average_latency": 0.0012,  # In s
#                                         "tail_latency": 0.0021,  # In s
#                                     },
#                                     "provider_1:region_2": {
#                                         "transmission_count": 44,
#                                         "average_latency": 0.13,  # In s
#                                         "tail_latency": 0.16,  # In s
#                                     },
#                                 },
#                                 "provider_1:region_2": {
#                                     "provider_1:region_1": {
#                                         "transmission_count": 16,
#                                         "average_latency": 0.09,  # In s
#                                         "tail_latency": 0.13,  # In s
#                                     }
#                                 },
#                             },
#                         }
#                     },
#                 },
#                 "instance_2": {
#                     "invocation_count": 160,
#                     "execution_summary": {
#                         "provider_1:region_1": {
#                             "invocation_count": 116,
#                             "average_runtime": 12,  # In s
#                             "tail_runtime": 16,  # In s
#                         },
#                         "provider_1:region_2": {
#                             "invocation_count": 44,
#                             "average_runtime": 11,  # In s
#                             "tail_runtime": 16,  # In s
#                         },
#                     },
#                 },
#             },
#         },
#     }
