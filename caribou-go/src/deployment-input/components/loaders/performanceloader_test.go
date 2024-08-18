package loaders

import (
	"encoding/json"
	"testing"
)

const PerformanceData = `
{
	"aws:region1": {
		"relative_performance": 1,
		"transmission_latency": {
			"aws:region1": {"latency_distribution": [0.005], "unit": "s"},
			"aws:region2": {"latency_distribution": [0.05], "unit": "s"}
		}
	}
}
`

func TestGetRelativePerformance(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(PerformanceData), &data)
	loader, _ := NewPerformanceLoader(data)

	relativePerformance := loader.GetRelativePerformance("aws:region1")
	if relativePerformance != 1.0 {
		t.Errorf("expected 1.0, got %f", relativePerformance)
	}

	relativePerformance = loader.GetRelativePerformance("aws:non-existent-region")
	if relativePerformance != 1.0 {
		t.Errorf("expected 1.0, got %f", relativePerformance)
	}
}

func TestGetTransmissionLatencyDistribution(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(PerformanceData), &data)
	loader, _ := NewPerformanceLoader(data)

	latencyDistribution := loader.GetTransmissionLatencyDistribution("aws:region1", "aws:region2")
	expectedLatencyDistribution := []float64{0.05}
	for i, v := range latencyDistribution {
		if v != expectedLatencyDistribution[i] {
			t.Errorf("expected %f, got %f", expectedLatencyDistribution[i], v)
		}
	}

	latencyDistribution = loader.GetTransmissionLatencyDistribution("aws:region1", "aws:non-existent-region")
	expectedLatencyDistribution = []float64{1000.0}
	for i, v := range latencyDistribution {
		if v != expectedLatencyDistribution[i] {
			t.Errorf("expected %f, got %f", expectedLatencyDistribution[i], v)
		}
	}
}
