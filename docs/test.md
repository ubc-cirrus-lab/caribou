```json
{
    "daily_invocation_counts": {
        "2024-06-05+0000": 2,
        "2024-06-06+0000": 4
    },
    "daily_failure_counts": {},
    "logs": [
        {
            "run_id": "6447a5276d1549bb9ed2b7d5f2e59b0b",
            "start_time": "2024-06-05 21:05:56,500006+0000",
            "runtime": 8.103811,
            "execution_data": [
                {
                    "instance_name": "simple_join-0_0_1-start:entry_point:0",
                    "user_code_duration": 3.501037,
                    "duration": 3.626,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 5.8239325881004333e-05,
                    "cpu_utilization": 0.1095791462699945,
                    "relevant_insights": {
                        "cpu_total_time": 0.23,
                        "duration": 3.626,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1": {
                            "successor_instance_name": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                            "task_type": "INVOKE_SUCCESSOR_ONLY",
                            "invocation_time_from_function_start": 0.500758,
                            "destination_region": "aws:ca-west-1"
                        },
                        "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2": {
                            "successor_instance_name": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                            "task_type": "INVOKE_SUCCESSOR_ONLY",
                            "invocation_time_from_function_start": 3.50096,
                            "destination_region": "aws:ca-west-1"
                        }
                    }
                },
                {
                    "instance_name": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                    "user_code_duration": 0.200874,
                    "duration": 0.202,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 3.613904118537903e-05,
                    "cpu_utilization": 1.7959564511138615,
                    "relevant_insights": {
                        "cpu_total_time": 0.21,
                        "duration": 0.202,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-join:sync:": {
                            "successor_instance_name": "simple_join-0_0_1-join:sync:",
                            "task_type": "SYNC_UPLOAD_ONLY",
                            "invocation_time_from_function_start": 0.000724,
                            "destination_region": "aws:ca-west-1"
                        }
                    }
                },
                {
                    "instance_name": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                    "user_code_duration": 2.493265,
                    "duration": 2.495,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 0.00036614201962947845,
                    "cpu_utilization": 0.5885403619739479,
                    "relevant_insights": {
                        "cpu_total_time": 0.85,
                        "duration": 2.495,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-join:sync:": {
                            "successor_instance_name": "simple_join-0_0_1-join:sync:",
                            "task_type": "SYNC_UPLOAD_AND_INVOKE",
                            "invocation_time_from_function_start": 1.993122,
                            "destination_region": "aws:ca-west-1"
                        }
                    }
                },
                {
                    "instance_name": "simple_join-0_0_1-join:sync:",
                    "user_code_duration": 2.187793,
                    "duration": 2.189,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "download_information": {
                        "download_size": 2.7091242372989655e-05,
                        "download_time": 0.296562,
                        "consumed_read_capacity": 8.0
                    },
                    "data_transfer_during_execution": 0.00035007577389478683,
                    "cpu_utilization": 0.6076770571608041,
                    "relevant_insights": {
                        "cpu_total_time": 0.77,
                        "duration": 2.189,
                        "total_memory": 1024
                    }
                }
            ],
            "transmission_data": [
                {
                    "transmission_size": 3.7383288145065308e-06,
                    "transmission_latency": 0.177744,
                    "from_instance": "simple_join-0_0_1-start:entry_point:0",
                    "to_instance": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": true,
                    "from_direct_successor": true
                },
                {
                    "transmission_size": 3.7392601370811462e-06,
                    "transmission_latency": 0.167498,
                    "from_instance": "simple_join-0_0_1-start:entry_point:0",
                    "to_instance": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": true,
                    "from_direct_successor": true
                },
                {
                    "transmission_size": 3.621913492679596e-06,
                    "from_instance": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                    "to_instance": "simple_join-0_0_1-join:sync:",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": false,
                    "from_direct_successor": true,
                    "sync_information": {
                        "upload_size": 2.7009285986423492e-05,
                        "consumed_write_capacity": 31.0,
                        "sync_data_response_size": 6.379559636116028e-07
                    }
                },
                {
                    "transmission_size": 3.621913492679596e-06,
                    "transmission_latency": 0.254403,
                    "from_instance": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                    "to_instance": "simple_join-0_0_1-join:sync:",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": true,
                    "from_direct_successor": true,
                    "sync_information": {
                        "upload_size": 8.195638656616211e-08,
                        "consumed_write_capacity": 31.0,
                        "sync_data_response_size": 7.050111889839172e-07
                    }
                }
            ],
            "non_executions": {},
            "start_hop_info": {
                "instance_name": "simple_join-0_0_1-start:entry_point:0",
                "destination": "aws:ca-west-1",
                "data_transfer_size": 9.499490261077881e-08,
                "latency": 0.317311,
                "workflow_placement_decision": {
                    "data_size": 8.195638656616211e-08,
                    "consumed_read_capacity": 1.0
                }
            },
            "unique_cpu_models": [
                "Intel(R) Xeon(R) Processor @ 2.90GHz"
            ]
        },
        {
            "run_id": "c729a267d1e14220b26f262e4aea9cba",
            "start_time": "2024-06-06 19:19:07,237334+0000",
            "runtime": 6.228028,
            "execution_data": [
                {
                    "instance_name": "simple_join-0_0_1-start:entry_point:0",
                    "user_code_duration": 3.501167,
                    "duration": 4.018,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 6.408337503671646e-05,
                    "cpu_utilization": 0.14188349692010954,
                    "relevant_insights": {
                        "cpu_total_time": 0.33,
                        "duration": 4.018,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1": {
                            "successor_instance_name": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                            "task_type": "INVOKE_SUCCESSOR_ONLY",
                            "invocation_time_from_function_start": 0.500824,
                            "destination_region": "aws:ca-west-1"
                        },
                        "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2": {
                            "successor_instance_name": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                            "task_type": "CONDITIONALLY_NOT_INVOKE",
                            "invocation_time_from_function_start": 3.501082,
                            "destination_region": "aws:ca-west-1",
                            "invoking_sync_node_data_output": {
                                "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2 -> simple_join-0_0_1-join:sync:": 3.621913492679596e-06
                            }
                        }
                    }
                },
                {
                    "instance_name": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                    "user_code_duration": 0.201276,
                    "duration": 0.203,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 3.615580499172211e-05,
                    "cpu_utilization": 0.8510044642857143,
                    "relevant_insights": {
                        "cpu_total_time": 0.1,
                        "duration": 0.203,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-join:sync:": {
                            "successor_instance_name": "simple_join-0_0_1-join:sync:",
                            "task_type": "SYNC_UPLOAD_ONLY",
                            "invocation_time_from_function_start": 0.001134,
                            "destination_region": "aws:ca-west-1"
                        }
                    }
                },
                {
                    "instance_name": "simple_join-0_0_1-join:sync:",
                    "user_code_duration": 2.134857,
                    "duration": 2.136,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "download_information": {
                        "download_size": 2.7010217308998108e-05,
                        "download_time": 0.320632,
                        "consumed_read_capacity": 8.0
                    },
                    "data_transfer_during_execution": 0.00035125017166137695,
                    "cpu_utilization": 0.6470183754681649,
                    "relevant_insights": {
                        "cpu_total_time": 0.8,
                        "duration": 2.136,
                        "total_memory": 1024
                    }
                }
            ],
            "transmission_data": [
                {
                    "transmission_size": 3.7392601370811462e-06,
                    "transmission_latency": 0.211569,
                    "from_instance": "simple_join-0_0_1-start:entry_point:0",
                    "to_instance": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": true,
                    "from_direct_successor": true
                },
                {
                    "transmission_size": 3.621913492679596e-06,
                    "transmission_latency": 0.592051,
                    "from_instance": "simple_join-0_0_1-start:entry_point:0",
                    "uninvoked_instance": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                    "to_instance": "simple_join-0_0_1-join:sync:",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": true,
                    "from_direct_successor": false
                },
                {
                    "transmission_size": 3.621913492679596e-06,
                    "from_instance": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                    "to_instance": "simple_join-0_0_1-join:sync:",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": false,
                    "from_direct_successor": true,
                    "sync_information": {
                        "upload_size": 2.7010217308998108e-05,
                        "consumed_write_capacity": 31.0,
                        "sync_data_response_size": 6.379559636116028e-07
                    }
                }
            ],
            "non_executions": {
                "simple_join-0_0_1-start:entry_point:0": {
                    "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2": 1
                }
            },
            "start_hop_info": {
                "instance_name": "simple_join-0_0_1-start:entry_point:0",
                "destination": "aws:ca-west-1",
                "data_transfer_size": 9.592622518539429e-08,
                "latency": 0.00057,
                "workflow_placement_decision": {
                    "data_size": 8.288770914077759e-08,
                    "consumed_read_capacity": 1.0
                }
            },
            "unique_cpu_models": [
                "Intel(R) Xeon(R) Processor @ 2.90GHz"
            ]
        },
        {
            "run_id": "fd36940b41274bc4b151353d3cc52a68",
            "start_time": "2024-06-06 19:19:21,186884+0000",
            "runtime": 7.922962,
            "execution_data": [
                {
                    "instance_name": "simple_join-0_0_1-start:entry_point:0",
                    "user_code_duration": 3.501144,
                    "duration": 3.625,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 4.175584763288498e-05,
                    "cpu_utilization": 0.06671875000000002,
                    "relevant_insights": {
                        "cpu_total_time": 0.14,
                        "duration": 3.625,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1": {
                            "successor_instance_name": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                            "task_type": "CONDITIONALLY_NOT_INVOKE",
                            "invocation_time_from_function_start": 0.500823,
                            "destination_region": "aws:ca-west-1"
                        },
                        "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2": {
                            "successor_instance_name": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                            "task_type": "INVOKE_SUCCESSOR_ONLY",
                            "invocation_time_from_function_start": 3.501065,
                            "destination_region": "aws:ca-west-1"
                        }
                    }
                },
                {
                    "instance_name": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                    "user_code_duration": 2.460534,
                    "duration": 2.462,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 0.0003659455105662346,
                    "cpu_utilization": 0.6034458138708367,
                    "relevant_insights": {
                        "cpu_total_time": 0.86,
                        "duration": 2.462,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-join:sync:": {
                            "successor_instance_name": "simple_join-0_0_1-join:sync:",
                            "task_type": "SYNC_UPLOAD_AND_INVOKE",
                            "invocation_time_from_function_start": 1.960371,
                            "destination_region": "aws:ca-west-1"
                        }
                    }
                },
                {
                    "instance_name": "simple_join-0_0_1-join:sync:",
                    "user_code_duration": 1.992976,
                    "duration": 1.994,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "download_information": {
                        "download_size": 8.288770914077759e-08,
                        "download_time": 0.149521,
                        "consumed_read_capacity": 1.0
                    },
                    "data_transfer_during_execution": 0.0003387127071619034,
                    "cpu_utilization": 0.7104222824724172,
                    "relevant_insights": {
                        "cpu_total_time": 0.82,
                        "duration": 1.994,
                        "total_memory": 1024
                    }
                }
            ],
            "transmission_data": [
                {
                    "transmission_size": 3.7401914596557617e-06,
                    "transmission_latency": 0.18126,
                    "from_instance": "simple_join-0_0_1-start:entry_point:0",
                    "to_instance": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": true,
                    "from_direct_successor": true
                },
                {
                    "transmission_size": 3.621913492679596e-06,
                    "transmission_latency": 0.287251,
                    "from_instance": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                    "to_instance": "simple_join-0_0_1-join:sync:",
                    "from_region": "aws:ca-west-1",
                    "to_region": "aws:ca-west-1",
                    "successor_invoked": true,
                    "from_direct_successor": true,
                    "sync_information": {
                        "upload_size": 8.288770914077759e-08,
                        "consumed_write_capacity": 3.0,
                        "sync_data_response_size": 7.068738341331482e-07
                    }
                }
            ],
            "non_executions": {
                "simple_join-0_0_1-start:entry_point:0": {
                    "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1": 1
                }
            },
            "start_hop_info": {
                "instance_name": "simple_join-0_0_1-start:entry_point:0",
                "destination": "aws:ca-west-1",
                "data_transfer_size": 9.592622518539429e-08,
                "latency": 0.03552,
                "workflow_placement_decision": {
                    "data_size": 8.288770914077759e-08,
                    "consumed_read_capacity": 1.0
                }
            },
            "unique_cpu_models": [
                "Intel(R) Xeon(R) Processor @ 2.90GHz"
            ]
        },
        {
            "run_id": "5826630f3e38431dbef00a07a921e24d",
            "start_time": "2024-06-06 19:19:33,885748+0000",
            "runtime": 3.650293,
            "execution_data": [
                {
                    "instance_name": "simple_join-0_0_1-start:entry_point:0",
                    "user_code_duration": 3.501293,
                    "duration": 3.651,
                    "cpu_model": "Intel(R) Xeon(R) Processor @ 2.90GHz",
                    "provider_region": "aws:ca-west-1",
                    "data_transfer_during_execution": 3.700237721204758e-05,
                    "cpu_utilization": 0.10882880974390578,
                    "relevant_insights": {
                        "cpu_total_time": 0.23,
                        "duration": 3.651,
                        "total_memory": 1024
                    },
                    "successor_data": {
                        "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1": {
                            "successor_instance_name": "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1",
                            "task_type": "CONDITIONALLY_NOT_INVOKE",
                            "invocation_time_from_function_start": 0.500969,
                            "destination_region": "aws:ca-west-1"
                        },
                        "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2": {
                            "successor_instance_name": "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2",
                            "task_type": "CONDITIONALLY_NOT_INVOKE",
                            "invocation_time_from_function_start": 3.501215,
                            "destination_region": "aws:ca-west-1"
                        }
                    }
                }
            ],
            "transmission_data": [],
            "non_executions": {
                "simple_join-0_0_1-start:entry_point:0": {
                    "simple_join-0_0_1-left:simple_join-0_0_1-start_0_0:1": 1,
                    "simple_join-0_0_1-right:simple_join-0_0_1-start_0_1:2": 1
                }
            },
            "start_hop_info": {
                "instance_name": "simple_join-0_0_1-start:entry_point:0",
                "destination": "aws:ca-west-1",
                "data_transfer_size": 9.685754776000977e-08,
                "latency": 0.005634,
                "workflow_placement_decision": {
                    "data_size": 8.381903171539307e-08,
                    "consumed_read_capacity": 1.0
                }
            },
            "unique_cpu_models": [
                "Intel(R) Xeon(R) Processor @ 2.90GHz"
            ]
        }
    ],
    "workflow_runtime_samples": [
        8.103811,
        6.228028,
        7.922962,
        3.650293
    ],
    "last_sync_time": "2024-06-06 20:30:06,167812+0000"
}
```