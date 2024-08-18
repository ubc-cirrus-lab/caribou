package loaders

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

const DatacenterData = `
{
	"aws:ca-central-1": {
		"execution_cost": {
			"invocation_cost": {"arm64": 2e-07, "x86_64": 2e-07, "free_tier_invocations": 1000000},
			"compute_cost": {"arm64": 1.33334e-05, "x86_64": 1.66667e-05, "free_tier_compute_gb_s": 400000},
			"unit": "USD"
		},
		"transmission_cost": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"},
		"sns_cost": {"request_cost": 5e-07, "unit": "USD/requests"},
		"dynamodb_cost": {"read_cost": 2.75e-07, "write_cost": 1.375e-06, "storage_cost": 0.11, "unit": "USD"},
		"ecr_cost": {"storage_cost": 0.1, "unit": "USD"},
		"pue": 1.11,
		"cfe": 0.0,
		"average_memory_power": 3.92e-06,
		"max_cpu_power_kWh": 0.0035,
		"min_cpu_power_kWh": 0.00074,
		"available_architectures": ["arm64", "x86_64"]
	}
}
`

const (
	AwsRegion     = "aws:ca-central-1"
	UnknownRegion = "unknown-region"
)

func TestGetAverageMemoryPower(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	averageMemoryPower := loader.GetAverageMemoryPower(AwsRegion)
	assert.Equal(t, averageMemoryPower, 3.92e-6, "expected 3.92e-6, got %f", averageMemoryPower)

	averageMemoryPower = loader.GetAverageMemoryPower(UnknownRegion)
	assert.Equal(t, averageMemoryPower, SolverInputAverageMemoryPowerDefault, "expected %f, got %f", SolverInputAverageMemoryPowerDefault, averageMemoryPower)
}

func TestGetPue(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	pue := loader.GetPue(AwsRegion)
	assert.Equal(t, pue, 1.11, "expected 1.11, got %f", pue)

	pue = loader.GetPue(UnknownRegion)
	assert.Equal(t, pue, SolverInputPueDefault, "expected %f, got %f", SolverInputPueDefault, pue)
}

func TestGetCfe(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	cfe := loader.GetCfe(AwsRegion)
	assert.Equal(t, cfe, 0.0, "expected 0.0, got %f", cfe)
}

func TestGetMaxCpuPower(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	result := loader.GetMaxCpuPower(AwsRegion)
	assert.Equal(t, result, 0.0035)

	result = loader.GetMaxCpuPower(UnknownRegion)
	assert.Equal(t, result, SolverInputMaxCpuPowerDefault)
}

func TestGetMinCpuPower(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	result := loader.GetMinCpuPower(AwsRegion)
	assert.Equal(t, result, 0.00074)

	result = loader.GetMinCpuPower(UnknownRegion)
	assert.Equal(t, result, SolverInputMinCpuPowerDefault)
}

func TestDataCenterLoader_GetSNSRequestCost(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	result := loader.GetSNSRequestCost(AwsRegion)
	assert.Equal(t, result, 5e-07)

	// Test default value
	result = loader.GetSNSRequestCost(UnknownRegion)
	assert.Equal(t, result, SolverInputSnsRequestCostDefault)
}

func TestGet_dynamodb_read_write_cost(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	r, w := loader.GetDynamoDBReadWriteCost(AwsRegion)
	assert.Equal(t, r, 2.75e-07)
	assert.Equal(t, w, 1.375e-06)

	// Test default value
	r, w = loader.GetDynamoDBReadWriteCost(UnknownRegion)
	assert.Equal(t, r, SolverInputDynamodbReadCostDefault)
	assert.Equal(t, w, SolverInputDynamodbWriteCostDefault)
}

func TestGet_ecr_storage_cost(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	result := loader.GetEcrStorageCost(AwsRegion)
	assert.Equal(t, result, 0.1)

	// Test default value
	result = loader.GetEcrStorageCost(UnknownRegion)
	assert.Equal(t, result, SolverInputEcrMonthlyStorageCostDefault)
}

func TestGetComputeCost(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	computeCostArm64 := loader.GetComputeCost(AwsRegion, "arm64")
	assert.Equal(t, computeCostArm64, 1.33334e-05, "expected 1.33334e-05, got %f", computeCostArm64)

	computeCostDefault := loader.GetComputeCost(UnknownRegion, "x86_64")
	assert.Equal(t, computeCostDefault, SolverInputComputeCostDefault, "expected %f, got %f", SolverInputComputeCostDefault, computeCostDefault)
}

func TestGetInvocationCost(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	invocationCostArm64 := loader.GetInvocationCost(AwsRegion, "arm64")
	assert.Equal(t, invocationCostArm64, 2e-7, "expected 2e-7, got %f", invocationCostArm64)

	invocationCostDefault := loader.GetInvocationCost(UnknownRegion, "x86_64")
	assert.Equal(t, invocationCostDefault, SolverInputInvocationCostDefault, "expected %f, got %f", SolverInputInvocationCostDefault, invocationCostDefault)
}

func TestGetTransmissionCost(t *testing.T) {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(DatacenterData), &data)
	loader, _ := NewDataCenterLoader(data)
	transmissionCostGlobal := loader.GetTransmissionCost(AwsRegion, false)
	assert.Equal(t, transmissionCostGlobal, 0.09, "expected 0.09, got %f", transmissionCostGlobal)

	transmissionCostProvider := loader.GetTransmissionCost(AwsRegion, true)
	assert.Equal(t, transmissionCostProvider, 0.02, "expected 0.02, got %f", transmissionCostProvider)
}
