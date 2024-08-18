package calculators

import (
	"math"
	"reflect"
	"testing"

	"bou.ke/monkey"
	"caribou-go/src/deployment-input/components/loaders"
	"github.com/stretchr/testify/assert"
)

func TestCostCalculator_CalculateVirtualStartInstanceCost(t *testing.T) {
	defer monkey.UnpatchAll()
	datacenterLoader, _ := loaders.NewDataCenterLoader(map[string]interface{}{})
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetDynamoDBReadWriteCost", func(dl *loaders.DataCenterLoader, regionName string) (float64, float64) { return 0.02, 0.03 })
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetSNSRequestCost", func(dl *loaders.DataCenterLoader, regionName string) float64 { return 0.001 })

	// Define the test data
	snsDataCallAndOutputSizes := map[string][]float64{"aws:us-east-1": {0.005, 0.01}}
	dynamodbReadCapacity := 100.0
	dynamodbWriteCapacity := 200.0

	costCalculator, _ := NewCostCalculator(datacenterLoader, nil, nil)
	cost := costCalculator.CalculateVirtualStartInstanceCost(
		map[string]float64{},
		snsDataCallAndOutputSizes,
		dynamodbReadCapacity,
		dynamodbWriteCapacity,
	)

	expectedDynamodbCost := 100*0.02 + 200*0.03
	expectedSnsCost := 0.0
	for _, size := range snsDataCallAndOutputSizes["aws:us-east-1"] {
		expectedDynamodbCost += math.Ceil(size*1024*1024/64) * 0.001
	}
	expectedCost := expectedDynamodbCost + expectedSnsCost

	// Assert the cost calculation is correct
	assert.Equal(t, cost, expectedCost)
}

func TestCostCalculator_CalculateInstanceCost_Invoked(t *testing.T) {
	defer monkey.UnpatchAll()
	// Mock the methods for DynamoDB, SNS, execution costs, and data transfer
	costCalculator, _ := NewCostCalculator(nil, nil, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(costCalculator), "CalculateExecutionCost", func(cc *CostCalculator, instanceName string, regionName string, executionTime float64) float64 {
		return 0.5
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(costCalculator), "CalculateSNSCost", func(cc *CostCalculator, currentRegionName string, snsDataOutputSizes map[string][]float64) float64 {
		return 0.3
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(costCalculator), "CalculateDataTransferCost", func(cc *CostCalculator, currentRegionName string, dataOutputSizes map[string]float64) float64 {
		return 0.4
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(costCalculator), "CalculateDynamoDBCost", func(cc *CostCalculator, currentRegionName string, dynamodbReadCapacity float64, dynamodbWriteCapacity float64) float64 {
		return 0.2
	})

	// Define the test data
	executionTime := 100.0
	instanceName := "test_instance"
	currentRegionName := "aws:us-west-2"
	dataOutputSizes := map[string]float64{"aws:us-east-1": 0.1}
	snsDataCallAndOutputSizes := map[string][]float64{"aws:us-east-1": {0.02, 0.04}}
	dynamodbReadCapacity := 50.0
	dynamodbWriteCapacity := 100.0
	isInvoked := true

	// Call the method under test
	cost := costCalculator.CalculateInstanceCost(
		executionTime,
		instanceName,
		currentRegionName,
		dataOutputSizes,
		snsDataCallAndOutputSizes,
		dynamodbReadCapacity,
		dynamodbWriteCapacity,
		isInvoked,
	)

	// Calculate expected cost
	expectedCost := 0.5 + 0.3 + 0.4 + 0.2

	// Assert the cost calculation is correct
	assert.InDelta(t, cost, expectedCost, 0.001)
}

func TestCostCalculator_CalculateInstanceCost_NotInvoked(t *testing.T) {
	defer monkey.UnpatchAll()
	// Mock the methods for data transfer and DynamoDB costs
	costCalculator, _ := NewCostCalculator(nil, nil, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(costCalculator), "CalculateDataTransferCost", func(cc *CostCalculator, currentRegionName string, dataOutputSizes map[string]float64) float64 {
		return 0.4
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(costCalculator), "CalculateDynamoDBCost", func(cc *CostCalculator, currentRegionName string, dynamodbReadCapacity float64, dynamodbWriteCapacity float64) float64 {
		return 0.2
	})

	// Define the test data
	executionTime := 0.0
	instanceName := "test_instance"
	currentRegionName := "aws:us-west-2"
	dataOutputSizes := map[string]float64{"aws:us-east-1": 0.1}
	snsDataCallAndOutputSizes := map[string][]float64{"aws:us-east-1": {0.02, 0.04}}
	dynamodbReadCapacity := 50.0
	dynamodbWriteCapacity := 100.0
	isInvoked := false

	// Call the method under test
	cost := costCalculator.CalculateInstanceCost(
		executionTime,
		instanceName,
		currentRegionName,
		dataOutputSizes,
		snsDataCallAndOutputSizes,
		dynamodbReadCapacity,
		dynamodbWriteCapacity,
		isInvoked,
	)

	// Calculate expected cost
	expectedCost := 0.4 + 0.2

	// Assert the cost calculation is correct
	assert.InDelta(t, cost, expectedCost, 0.001)
}

func TestCostCalculator_CalculateDynamoDBCost(t *testing.T) {
	// Mock the methods for DynamoDB read/write costs
	defer monkey.UnpatchAll()
	datacenterLoader, _ := loaders.NewDataCenterLoader(map[string]interface{}{})
	costCalculator, _ := NewCostCalculator(datacenterLoader, nil, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetDynamoDBReadWriteCost", func(dl *loaders.DataCenterLoader, regionName string) (float64, float64) { return 0.02, 0.03 })

	// Define the test data
	currentRegionName := "aws:us-east-1"
	dynamodbReadCapacity := 50.0
	dynamodbWriteCapacity := 100.0

	// Call the private method under test
	cost := costCalculator.CalculateDynamoDBCost(
		currentRegionName, dynamodbReadCapacity, dynamodbWriteCapacity,
	)

	// Calculate expected cost
	expectedCost := 50*0.02 + 100*0.03

	// Assert the cost calculation is correct
	assert.Equal(t, cost, expectedCost)
}

func TestCostCalculator_CalculateSNSCost(t *testing.T) {
	// Mock the SNS request cost and transmission cost methods
	defer monkey.UnpatchAll()
	datacenterLoader, _ := loaders.NewDataCenterLoader(map[string]interface{}{})
	costCalculator, _ := NewCostCalculator(datacenterLoader, nil, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetSNSRequestCost", func(dl *loaders.DataCenterLoader, regionName string) float64 { return 0.001 })
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetTransmissionCost", func(dl *loaders.DataCenterLoader, regionName string, intraProviderTransfer bool) float64 { return 0.05 })

	// Define the test data
	currentRegionName := "aws:us-west-2"
	snsDataCallAndOutputSizes := map[string][]float64{"aws:us-west-2": {0.005, 0.01}, "aws:us-east-1": {0.002, 0.004}}

	// Call the private method under test
	cost := costCalculator.CalculateSNSCost(currentRegionName, snsDataCallAndOutputSizes)

	// Calculate expected cost
	expectedCost := 0.0
	// Account for intra-region SNS data transfer
	if costCalculator.ConsiderIntraRegionTransferForSNS {
		for _, cost := range snsDataCallAndOutputSizes["aws:us-west-2"] {
			expectedCost += cost * 0.05
		}
	}
	// Calculate SNS invocation costs
	for _, sizes := range snsDataCallAndOutputSizes {
		for _, size := range sizes {
			requests := math.Ceil(size * 1024 * 1024 / 64)
			expectedCost += requests * 0.001
		}
	}

	// Assert the cost calculation is correct using assertAlmostEqual for precision
	assert.InDelta(t, cost, expectedCost, 0.0001)
}

func TestCostCalculator_CalculateSNSCost_EmptyData(t *testing.T) {
	// Mock the SNS request cost and transmission cost methods
	defer monkey.UnpatchAll()
	datacenterLoader, _ := loaders.NewDataCenterLoader(map[string]interface{}{})
	costCalculator, _ := NewCostCalculator(datacenterLoader, nil, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetSNSRequestCost", func(dl *loaders.DataCenterLoader, regionName string) float64 { return 0.001 })
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetTransmissionCost", func(dl *loaders.DataCenterLoader, regionName string, intraProviderTransfer bool) float64 { return 0.05 })

	// Define the test data
	currentRegionName := "aws:us-west-2"
	snsDataCallAndOutputSizes := map[string][]float64{}

	// Call the private method under test
	cost := costCalculator.CalculateSNSCost(currentRegionName, snsDataCallAndOutputSizes)

	// Assert the cost calculation is correct using assertAlmostEqual for precision
	assert.Equal(t, cost, 0.0)
}

func TestCostCalculator_CalculateDataTransferCost(t *testing.T) {
	// Mock the transmission cost method
	defer monkey.UnpatchAll()
	datacenterLoader, _ := loaders.NewDataCenterLoader(map[string]interface{}{})
	costCalculator, _ := NewCostCalculator(datacenterLoader, nil, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetTransmissionCost", func(dl *loaders.DataCenterLoader, regionName string, intraProviderTransfer bool) float64 { return 0.05 })

	// Define the test data
	currentRegionName := "aws:us-west-2"
	dataOutputSizes := map[string]float64{"aws:us-east-1": 0.1, "aws:us-west-2": 0.2}

	// Call the private method under test
	cost := costCalculator.CalculateDataTransferCost(currentRegionName, dataOutputSizes)

	// Calculate expected cost
	expectedCost := 0.1 * 0.05 // Only count data transfer out of the current region

	// Assert the cost calculation is correct
	assert.InDelta(t, cost, expectedCost, 0.000001)
}

func TestCostCalculator_CalculateExecutionCost(t *testing.T) {
	// Mock the method for execution conversion ratio
	defer monkey.UnpatchAll()
	costCalculator, _ := NewCostCalculator(nil, nil, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(costCalculator), "GetExecutionConversionRatio", func(calc *CostCalculator, instanceName string, regionName string) (float64, float64) {
		return 0.1, 0.05
	})

	// Define the test data
	instanceName := "test_instance"
	regionName := "aws:us-west-2"
	executionTime := 10.0

	// Call the private method under test
	cost := costCalculator.CalculateExecutionCost(instanceName, regionName, executionTime)

	// Calculate expected cost
	expectedCost := 0.1*10.0 + 0.05

	// Assert the cost calculation is correct
	assert.Equal(t, cost, expectedCost)
}

func TestCostCalculator_GetExecutionConversionRatio(t *testing.T) {
	// Mock the method for getting memory and architecture
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, "", nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetMemory", func(wl *loaders.WorkflowLoader, instanceName string, providerName string) float64 { return 2048.0 })
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetArchitecture", func(wl *loaders.WorkflowLoader, instanceName string, providerName string) string { return "x86_64" })
	datacenterLoader, _ := loaders.NewDataCenterLoader(map[string]interface{}{})
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetComputeCost", func(dl *loaders.DataCenterLoader, regionName string, architecture string) float64 { return 0.2 })
	monkey.PatchInstanceMethod(reflect.TypeOf(datacenterLoader), "GetInvocationCost", func(dl *loaders.DataCenterLoader, regionName string, architecture string) float64 { return 0.05 })
	costCalculator, _ := NewCostCalculator(datacenterLoader, workflowLoader, nil)
	// Define the test data
	instanceName := "test_instance"
	regionName := "aws:us-west-2"

	// Call the private method under test
	compute, invocation := costCalculator.GetExecutionConversionRatio(instanceName, regionName)

	// Calculate expected ratio
	expectedCompute, expectedInvocation := 0.2*(2048/1024), 0.05

	// Assert the conversion ratio calculation is correct
	assert.Equal(t, compute, expectedCompute)
	assert.Equal(t, invocation, expectedInvocation)

	// Assert the cache is updated correctly
	cacheKey := instanceName + "_" + regionName
	_, exist := costCalculator.executionConversionRatioCache[cacheKey]
	assert.True(t, exist)
}
