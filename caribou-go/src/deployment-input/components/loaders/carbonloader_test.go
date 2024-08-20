package loaders

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

const CarbonData = `
{
	"aws:eu-south-1": {
		"averages": {
			"overall": {"carbon_intensity": 482.0},
			"0": {"carbon_intensity": 482.0},
			"1": {"carbon_intensity": 482.0},
			"2": {"carbon_intensity": 482.0},
			"3": {"carbon_intensity": 482.0},
			"4": {"carbon_intensity": 482.0},
			"5": {"carbon_intensity": 498.0},
			"6": {"carbon_intensity": 482.0},
			"7": {"carbon_intensity": 482.0},
			"8": {"carbon_intensity": 482.0},
			"9": {"carbon_intensity": 482.0},
			"10": {"carbon_intensity": 482.0},
			"11": {"carbon_intensity": 482.0},
			"12": {"carbon_intensity": 482.0},
			"13": {"carbon_intensity": 482.0},
			"14": {"carbon_intensity": 482.0},
			"15": {"carbon_intensity": 482.0},
			"16": {"carbon_intensity": 482.0},
			"17": {"carbon_intensity": 482.0},
			"18": {"carbon_intensity": 482.0},
			"19": {"carbon_intensity": 482.0},
			"20": {"carbon_intensity": 482.0},
			"21": {"carbon_intensity": 482.0},
			"22": {"carbon_intensity": 482.0},
			"23": {"carbon_intensity": 482.0}
		},
		"units": "gCO2eq/kWh",
		"transmission_distances": {"aws:eu-south-1": 0, "aws:eu-south-2": 111.19},
		"transmission_distances_unit": "km"
	}
}
`

func TestGetTransmissionDistance(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(CarbonData), &data)
	loader, _ := NewCarbonLoader(data)
	distance := loader.GetTransmissionDistance("aws:eu-south-1", "aws:eu-south-2")
	if distance != 111.19 {
		t.Errorf("expected 111.19, got %f", distance)
	}

	distance = loader.GetTransmissionDistance("aws:eu-south-1", "aws:eu-south-1")
	if distance != 0 {
		t.Errorf("expected 0, got %f", distance)
	}

	distance = loader.GetTransmissionDistance("aws:eu-south-1", "aws:non-existent-region")
	if distance != -1 {
		t.Errorf("expected -1, got %f", distance)
	}
}

func TestGetGridCarbonIntensity(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(CarbonData), &data)
	loader, _ := NewCarbonLoader(data)
	overallCarbonIntensity := loader.GetGridCarbonIntensity("aws:eu-south-1", nil)
	assert.Equal(t, 482.0, overallCarbonIntensity, "expected 482.0, got %f", overallCarbonIntensity)

	hour := "5"
	hourlyCarbonIntensity := loader.GetGridCarbonIntensity("aws:eu-south-1", &hour)
	assert.Equal(t, 498.0, hourlyCarbonIntensity, "expected 498.0, got %f", hourlyCarbonIntensity)

	hour = "non-existent-hour"
	hourlyCarbonIntensity = loader.GetGridCarbonIntensity("aws:eu-south-1", &hour)
	assert.Equal(t, 500.0, hourlyCarbonIntensity, "expected 500.0, got %f", hourlyCarbonIntensity)
}
