package loaders

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

const WorkflowData = `
{
    "workflow_runtime_samples": [
        5.001393,
        5.001395,
        5.511638
    ],
    "daily_invocation_counts": {
        "2024-08-01+0000": 18
    },
    "start_hop_summary": {
        "invoked": 13,
        "retrieved_wpd_at_function": 11,
        "wpd_at_function_probability": 0.8461538461538461,
        "workflow_placement_decision_size_gb": 1.5972182154655457e-06,
        "from_client": {
            "transfer_sizes_gb": [
                1.9846484065055847e-06,
                1.9846484065055847e-06,
                2.1280720829963684e-06
            ],
            "received_region": {
                "aws:ca-west-1": {
                    "transfer_size_gb_to_transfer_latencies_s": {
                        "9.5367431640625e-06": [
                            0.31179,
                            0.16143,
                            0.250207
                        ]
                    },
                    "best_fit_line": {
                        "slope_s": 0.0,
                        "intercept_s": 0.236685125,
                        "min_latency_s": 0.16567958749999998,
                        "max_latency_s": 0.3076906625
                    }
                }
            }
        }
    },
    "instance_summary": {
        "simple_call-0_0_1-f1:entry_point:0": {
            "invocations": 13,
            "cpu_utilization": 0.060168300407512976,
            "executions": {
                "at_region": {
                    "aws:ca-west-1": {
                        "cpu_utilization": 0.0563360316574529,
                        "durations_s": [
                            5.002,
                            5.002,
                            5.025
                        ],
                        "auxiliary_data": {
                            "5.01": [
                                [
                                    3.6343932151794434e-05,
                                    0.001
                                ],
                                [
                                    1.291465014219284e-05,
                                    0.001
                                ]
                            ]
                        }
                    }
                }
            },
            "to_instance": {
                "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1": {
                    "invoked": 13,
                    "invocation_probability": 1.0,
                    "transfer_sizes_gb": [
                        1.9157305359840393e-06
                    ],
                    "regions_to_regions": {
                        "aws:ca-west-1": {
                            "aws:ca-west-1": {
                                "transfer_size_gb_to_transfer_latencies_s": {
                                    "9.5367431640625e-06": [
                                        0.27353,
                                        0.175446,
                                        0.188676
                                    ]
                                },
                                "best_fit_line": {
                                    "slope_s": 0.0,
                                    "intercept_s": 0.220738625,
                                    "min_latency_s": 0.15451703749999998,
                                    "max_latency_s": 0.2869602125
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
`

func TestGetRuntimeDistribution(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	loader, _ := NewWorkflowLoader(
		data, map[string]interface{}{},
		"aws:ca-west-1", nil,
	)

	instanceName := "simple_call-0_0_1-f1:entry_point:0"
	regionName := "aws:ca-west-1"
	runtimeDistribution := loader.GetRuntimeDistribution(instanceName, regionName, false)
	expectedDistribution := []float64{5.002, 5.002, 5.025}

	if len(runtimeDistribution) != len(expectedDistribution) {
		t.Errorf("expected length %d, got %d", len(expectedDistribution), len(runtimeDistribution))
	}

	for i, v := range runtimeDistribution {
		if v != expectedDistribution[i] {
			t.Errorf("expected %f, got %f", expectedDistribution[i], v)
		}
	}
}

func TestGetStartHopSizeDistribution(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	loader, _ := NewWorkflowLoader(
		data, map[string]interface{}{},
		"aws:ca-west-1", nil,
	)

	startHopSizeDistribution := loader.GetStartHopSizeDistribution()
	expectedDistribution := []float64{1.9846484065055847e-06, 1.9846484065055847e-06, 2.1280720829963684e-06}

	if len(startHopSizeDistribution) != len(expectedDistribution) {
		t.Errorf("expected length %d, got %d", len(expectedDistribution), len(startHopSizeDistribution))
	}

	for i, v := range startHopSizeDistribution {
		if v != expectedDistribution[i] {
			t.Errorf("expected %f, got %f", expectedDistribution[i], v)
		}
	}
}

func TestGetStartHopLatencyDistribution(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	loader, _ := NewWorkflowLoader(
		data, map[string]interface{}{},
		"aws:ca-west-1", nil,
	)

	toRegionName := "aws:ca-west-1"
	dataTransferSize := 9.5367431640625e-06
	latencyDistribution := loader.GetStartHopLatencyDistribution(toRegionName, dataTransferSize)
	expectedDistribution := []float64{0.31179, 0.16143, 0.250207}

	if len(latencyDistribution) != len(expectedDistribution) {
		t.Errorf("expected length %d, got %d", len(expectedDistribution), len(latencyDistribution))
	}

	for i, v := range latencyDistribution {
		if v != expectedDistribution[i] {
			t.Errorf("expected %f, got %f", expectedDistribution[i], v)
		}
	}
}

func TestGetDataTransferSizeDistribution(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	loader, _ := NewWorkflowLoader(
		data, map[string]interface{}{},
		"aws:ca-west-1", nil,
	)

	latencyDistribution := loader.GetDataTransferSizeDistribution(
		"simple_call-0_0_1-f1:entry_point:0", "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1",
	)
	expectedDistribution := []float64{1.9157305359840393e-06}

	if len(latencyDistribution) != len(expectedDistribution) {
		t.Errorf("expected length %d, got %d", len(expectedDistribution), len(latencyDistribution))
	}

	for i, v := range latencyDistribution {
		if v != expectedDistribution[i] {
			t.Errorf("expected %f, got %f", expectedDistribution[i], v)
		}
	}
}

func TestGetLatencyDistribution(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	loader, _ := NewWorkflowLoader(
		data, map[string]interface{}{},
		"", nil,
	)

	latencyDistribution := loader.GetLatencyDistribution(
		"simple_call-0_0_1-f1:entry_point:0",
		"simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1",
		"aws:ca-west-1",
		"aws:ca-west-1",
		9.5367431640625e-06,
	)
	expectedDistribution := []float64{0.27353, 0.175446, 0.188676}

	if len(latencyDistribution) != len(expectedDistribution) {
		t.Errorf("expected length %d, got %d", len(expectedDistribution), len(latencyDistribution))
	}

	for i, v := range latencyDistribution {
		if v != expectedDistribution[i] {
			t.Errorf("expected %f, got %f", expectedDistribution[i], v)
		}
	}
}

func TestGetInvocationProbability(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	loader, _ := NewWorkflowLoader(
		data, map[string]interface{}{},
		"", nil,
	)

	invocationProbability := loader.GetInvocationProbability("simple_call-0_0_1-f1:entry_point:0", "simple_call-0_0_1-f2:simple_call-0_0_1-f1_0_0:1")
	assert.Equal(t, invocationProbability, 1.0)
}

func TestGetVCpu(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	var dataIn map[string]interface{}
	_ = json.Unmarshal([]byte("{\"instance_1\": {\"provider_1\": {\"config\": {\"vcpu\": 2}}}}"), &dataIn)
	loader, _ := NewWorkflowLoader(
		data, dataIn,
		"", nil,
	)

	instanceName := "instance_1"
	providerName := "provider_1"
	vcpu := loader.GetVCpu(instanceName, providerName)
	assert.Equal(t, vcpu, 2.0)
}

func TestGetMemory(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	var dataIn map[string]interface{}
	_ = json.Unmarshal([]byte("{\"instance_1\": {\"provider_1\": {\"config\": {\"memory\": 1024}}}}"), &dataIn)
	loader, _ := NewWorkflowLoader(
		data, dataIn,
		"", nil,
	)

	instanceName := "instance_1"
	providerName := "provider_1"
	memory := loader.GetMemory(instanceName, providerName)
	expectedMemory := 1024.0

	if memory != expectedMemory {
		t.Errorf("expected %f, got %f", expectedMemory, memory)
	}
}

func TestGetArchitecture(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(WorkflowData), &data)
	var dataIn map[string]interface{}
	_ = json.Unmarshal([]byte("{\"instance_1\": {\"provider_1\": {\"config\": {\"architecture\": \"x86_64\"}}}}"), &dataIn)
	loader, _ := NewWorkflowLoader(
		data, dataIn,
		"", nil,
	)

	instanceName := "instance_1"
	providerName := "provider_1"
	architecture := loader.GetArchitecture(instanceName, providerName)
	expectedArchitecture := "x86_64"

	if architecture != expectedArchitecture {
		t.Errorf("expected %s, got %s", expectedArchitecture, architecture)
	}
}
