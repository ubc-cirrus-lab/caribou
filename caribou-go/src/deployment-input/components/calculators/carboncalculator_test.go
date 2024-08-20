package calculators

import (
	"reflect"
	"testing"

	"bou.ke/monkey"
	"caribou-go/src/deployment-input/components/loaders"
	"github.com/stretchr/testify/assert"
)

func TestCarbonCalculator_AlterCarbonSetting(t *testing.T) {
	carbonCalculator, _ := NewCarbonCalculator(
		nil, nil, nil, true, 0.001, false, false,
	)
	newSetting := "new_setting"
	carbonCalculator.AlterCarbonSetting(&newSetting)
	assert.Equal(t, *(carbonCalculator.hourlyCarbonSetting), "new_setting")
}

func TestCarbonCalculator_CalculateVirtualStartInstanceCarbon(t *testing.T) {
	defer monkey.UnpatchAll()
	carbonCalculator, _ := NewCarbonCalculator(
		nil, nil, nil, true, 0.001, false, false,
	)
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonCalculator), "CalculateDataTransferCarbon",
		func(
			calc *CarbonCalculator,
			currentRegionName string,
			dataInputSizes map[string]float64,
			dataOutputSizes map[string]float64,
			dataTransferDuringExecution float64,
		) float64 {
			assert.Equal(t, currentRegionName, "")
			_, exist := dataInputSizes["aws:us-west-2"]
			assert.True(t, exist)
			_, exist = dataOutputSizes["aws:us-east-1"]
			assert.True(t, exist)
			return 0.5
		})
	carbon := carbonCalculator.CalculateVirtualStartInstanceCarbon(
		map[string]float64{"aws:us-west-2": 10.0},
		map[string]float64{"aws:us-east-1": 5.0},
	)
	assert.Equal(t, carbon, 0.5)
}

func TestCarbonCalculator_CalculateInstanceCarbon(t *testing.T) {
	defer monkey.UnpatchAll()
	carbonCalculator, _ := NewCarbonCalculator(
		nil, nil, nil, true, 0.001, false, false,
	)
	executionTime := 100.0
	instanceName := "test_instance"
	regionName := "aws:us-west-2"
	dataInputSizes := map[string]float64{"aws:us-east-1": 5.0}
	dataOutputSizes := map[string]float64{"aws:us-east-1": 10.0}
	dataTransferDuringExecution := 2.0
	isInvoked := true
	isRedirector := false

	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonCalculator), "CalculateDataTransferCarbon",
		func(
			calc *CarbonCalculator,
			currentRegionName string,
			dataInputSizes map[string]float64,
			dataOutputSizes map[string]float64,
			dataTransferDuringExecution float64,
		) float64 {
			assert.Equal(t, currentRegionName, regionName)
			_, exist := dataInputSizes["aws:us-east-1"]
			assert.True(t, exist)
			_, exist = dataOutputSizes["aws:us-east-1"]
			assert.True(t, exist)
			assert.Equal(t, dataTransferDuringExecution, dataTransferDuringExecution)
			return 0.5
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonCalculator), "CalculateExecutionCarbon",
		func(
			cc *CarbonCalculator, instanceName string, regionName string,
			executionLatency float64, isRedirector bool,
		) float64 {
			assert.Equal(t, instanceName, instanceName)
			assert.Equal(t, regionName, regionName)
			assert.Equal(t, executionLatency, executionTime)
			assert.Equal(t, isRedirector, isRedirector)
			return 1.0
		})
	execution, transmission := carbonCalculator.CalculateInstanceCarbon(
		executionTime,
		instanceName,
		regionName,
		dataInputSizes,
		dataOutputSizes,
		dataTransferDuringExecution,
		isInvoked,
		isRedirector,
	)
	assert.Equal(t, execution, 1.0)
	assert.Equal(t, transmission, 0.5)
}

func TestCarbonCalculator_CalculateDataTransferCarbon_IntraRegionFree(t *testing.T) {
	defer monkey.UnpatchAll()
	dataInputSizes := map[string]float64{"aws:us-west-2": 10.0}
	dataOutputSizes := map[string]float64{"aws:us-west-2": 5.0}
	workflowLoader, _ := loaders.NewWorkflowLoader(
		map[string]interface{}{}, map[string]interface{}{}, "aws:us-east-1", nil,
	)
	carbonLoader, _ := loaders.NewCarbonLoader(map[string]interface{}{})
	carbonCalculator, _ := NewCarbonCalculator(
		carbonLoader, nil, workflowLoader, true, 0.001, true, false,
	)

	// Mock the carbon loader
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonLoader), "GetGridCarbonIntensity",
		func(c *loaders.CarbonLoader, regionName string, hour *string) float64 { return 0.3 })

	carbon := carbonCalculator.CalculateDataTransferCarbon(
		"aws:us-west-2", dataInputSizes, dataOutputSizes, 0.0,
	)

	// Since intra-region transmission is free, the carbon should be 0
	assert.Equal(t, carbon, 0.0)
}

func TestCarbonCalculator_CalculateDataTransferCarbon_InterRegion(t *testing.T) {
	defer monkey.UnpatchAll()
	dataInputSizes := map[string]float64{"aws:us-east-1": 10.0}
	dataOutputSizes := map[string]float64{"aws:us-west-2": 5.0}
	workflowLoader, _ := loaders.NewWorkflowLoader(
		map[string]interface{}{}, map[string]interface{}{}, "aws:us-east-1", nil,
	)
	carbonLoader, _ := loaders.NewCarbonLoader(map[string]interface{}{})
	carbonCalculator, _ := NewCarbonCalculator(
		carbonLoader, nil, workflowLoader, true, 0.001, false, false,
	)

	// Mock the carbon loader
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonCalculator), "GetNetworkCarbonIntensityRouteBetweenRegions",
		func(c *CarbonCalculator, regionOne string, regionTwo string) float64 { return 0.3 })

	carbon := carbonCalculator.CalculateDataTransferCarbon(
		"aws:us-west-2", dataInputSizes, dataOutputSizes, 0.0,
	)

	// Since intra-region transmission is free, the carbon should be 0
	assert.Equal(t, carbon, 10.0*0.001*0.3)
}

func TestCarbonCalculator_GetNetworkCarbonIntensityRouteBetweenRegions_Identical(t *testing.T) {
	defer monkey.UnpatchAll()
	workflowLoader, _ := loaders.NewWorkflowLoader(
		map[string]interface{}{}, map[string]interface{}{}, "aws:us-east-1", nil,
	)
	carbonLoader, _ := loaders.NewCarbonLoader(map[string]interface{}{})
	carbonCalculator, _ := NewCarbonCalculator(
		carbonLoader, nil, workflowLoader, true, 0.001, true, false,
	)

	// Mock the carbon loader
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonLoader), "GetGridCarbonIntensity",
		func(c *loaders.CarbonLoader, regionName string, hour *string) float64 { return 0.4 })

	carbon := carbonCalculator.GetNetworkCarbonIntensityRouteBetweenRegions(
		"aws:us-west-2", "aws:us-west-2",
	)

	// Since intra-region transmission is free, the carbon should be 0
	assert.Equal(t, carbon, 0.4)
}

func TestCarbonCalculator_GetNetworkCarbonIntensityRouteBetweenRegions_Different(t *testing.T) {
	defer monkey.UnpatchAll()
	workflowLoader, _ := loaders.NewWorkflowLoader(
		map[string]interface{}{}, map[string]interface{}{}, "aws:us-east-1", nil,
	)
	carbonLoader, _ := loaders.NewCarbonLoader(map[string]interface{}{})
	carbonCalculator, _ := NewCarbonCalculator(
		carbonLoader, nil, workflowLoader, true, 0.001, true, false,
	)

	// Mock the carbon loader
	carbonIntensityResults := []float64{0.4, 0.6}
	counter := 0
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonLoader), "GetGridCarbonIntensity",
		func(c *loaders.CarbonLoader, regionName string, hour *string) float64 {
			result := carbonIntensityResults[counter]
			counter += 1
			return result
		})

	carbon := carbonCalculator.GetNetworkCarbonIntensityRouteBetweenRegions(
		"aws:us-west-2", "aws:us-east-1",
	)

	// Since intra-region transmission is free, the carbon should be 0
	assert.Equal(t, carbon, 0.5)
}

func TestCalculateExecutionCarbon(t *testing.T) {
	defer monkey.UnpatchAll()
	instanceName := "test_instance"
	regionName := "aws:us-west-2"
	executionLatency := 50.0
	isRedirector := false
	carbonCalculator, _ := NewCarbonCalculator(
		nil, nil, nil, true, 0.001, false, false,
	)
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonCalculator), "GetExecutionConversionRatio",
		func(calc *CarbonCalculator, instanceName string, regionName string, isRedirector bool) []float64 {
			assert.Equal(t, instanceName, instanceName)
			assert.Equal(t, regionName, regionName)
			assert.Equal(t, isRedirector, isRedirector)
			return []float64{1.0, 2.0, 3.0}
		})
	result := carbonCalculator.CalculateExecutionCarbon(instanceName, regionName, executionLatency, isRedirector)
	assert.Equal(t, result, executionLatency*(1.0+2.0)*3.0)
}

func TestGetExecutionConversionRatio(t *testing.T) {
	defer monkey.UnpatchAll()
	carbonLoader, _ := loaders.NewCarbonLoader(map[string]interface{}{})
	datacenterLoader, _ := loaders.NewDataCenterLoader(map[string]interface{}{})
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, "", nil)
	carbonCalculator, _ := NewCarbonCalculator(
		carbonLoader, datacenterLoader, workflowLoader, true, 0.001, false, false,
	)
	instanceName := "test_instance"
	regionName := "aws:us-west-2"
	isRedirector := false
	monkey.PatchInstanceMethod(
		reflect.TypeOf(datacenterLoader), "GetAverageMemoryPower",
		func(dl *loaders.DataCenterLoader, regionName string) float64 {
			return 0.5
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(datacenterLoader), "GetCfe",
		func(dl *loaders.DataCenterLoader, regionName string) float64 {
			return 0.2
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(datacenterLoader), "GetPue",
		func(dl *loaders.DataCenterLoader, regionName string) float64 {
			return 1.1
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(carbonLoader), "GetGridCarbonIntensity",
		func(dl *loaders.CarbonLoader, regionName string, hour *string) float64 {
			return 0.3
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(workflowLoader), "GetVCpu",
		func(wl *loaders.WorkflowLoader, instanceName string, providerName string) float64 {
			return 2.0
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(workflowLoader), "GetMemory",
		func(wl *loaders.WorkflowLoader, instanceName string, providerName string) float64 {
			return 4096
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(datacenterLoader), "GetMinCpuPower",
		func(dl *loaders.DataCenterLoader, regionName string) float64 {
			return 0.1
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(datacenterLoader), "GetMaxCpuPower",
		func(dl *loaders.DataCenterLoader, regionName string) float64 {
			return 0.5
		})
	monkey.PatchInstanceMethod(
		reflect.TypeOf(workflowLoader), "GetAverageCpuUtilization",
		func(wl *loaders.WorkflowLoader, instanceName string, regionName string, isRedirector bool) float64 {
			return 0.6
		})

	// Calculate expected values
	memoryGb := 4096.0 / 1024
	utilization := 0.6
	minCpuPower := 0.1
	maxCpuPower := 0.5
	averageCpuPower := minCpuPower + utilization*(maxCpuPower-minCpuPower)
	expectedComputeFactor := averageCpuPower * 2.0 / 3600
	expectedMemoryFactor := 0.5 * memoryGb / 3600
	expectedPowerFactor := (1 - 0.2) * 1.1 * 0.3
	result := carbonCalculator.GetExecutionConversionRatio(instanceName, regionName, isRedirector)
	expected := []float64{expectedComputeFactor, expectedMemoryFactor, expectedPowerFactor}
	for i, r := range result {
		assert.InDelta(t, r, expected[i], 0.0001)
	}
}
