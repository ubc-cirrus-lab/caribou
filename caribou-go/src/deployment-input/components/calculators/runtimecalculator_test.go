package calculators

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"bou.ke/monkey"
	"caribou-go/src/deployment-input/components/loaders"
	"caribou-go/src/models"

	"github.com/stretchr/testify/assert"
)

func TestRuntimeCalculator_CalculateTransmissionSizeLatency(t *testing.T) {
	// Mock the distribution methods
	defer monkey.UnpatchAll()
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, "", nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetDataTransferSizeDistribution", func(wl *loaders.WorkflowLoader, from string, to string) []float64 { return []float64{0.1, 0.2, 0.3} })
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetLatencyDistribution", func(wl *loaders.WorkflowLoader, fromI string, toI string, fromR string, toR string, size float64) []float64 {
		return []float64{0.4, 0.5, 0.6}
	})
	monkey.Patch(rand.Int, func() int { return 0 })

	runtimeCalculator, _ := NewRuntimeCalculator(nil, workflowLoader)

	// Call the method
	fromInstanceName := "instance1"
	toInstanceName := "instance2"
	fromRegionName := "region1"
	toRegionName := "region2"
	transmissionSize, transmissionLatency := runtimeCalculator.CalculateTransmissionSizeLatency(
		fromInstanceName,
		fromRegionName,
		toInstanceName,
		toRegionName,
		false,
		false,
	)

	// Verify results
	assert.Equal(t, transmissionSize, 0.1)
	assert.Equal(t, transmissionLatency, 0.4)
}

func TestRuntimeCalculator_CalculateSimulatedTransmissionSizeLatency(t *testing.T) {
	// Mock the distribution methods
	defer monkey.UnpatchAll()
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, "", nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetNonExecutionSNSTransferSize", func(wl *loaders.WorkflowLoader, fromInstanceName string, toInstanceName string, syncToFromInstance string) float64 {
		return 0.1
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetNonExecutionTransferLatencyDistribution", func(wl *loaders.WorkflowLoader, fromInstanceName string, toInstanceName string, syncToFromInstance string, fromRegionName string, toRegionName string) []float64 {
		return []float64{0.2, 0.3, 0.4}
	})
	monkey.Patch(rand.Int, func() int { return 0 })

	runtimeCalculator, _ := NewRuntimeCalculator(nil, workflowLoader)

	// Call the method
	fromInstanceName := "instance1"
	toInstanceName := "instance2"
	fromRegionName := "region1"
	toRegionName := "region2"
	size, latency := runtimeCalculator.CalculateSimulatedTransmissionSizeLatency(
		fromInstanceName,
		toInstanceName,
		fromInstanceName,
		toInstanceName,
		fromRegionName,
		toRegionName,
	)

	// Verify results
	assert.Equal(t, size, 0.1)
	assert.Equal(t, latency, 0.2)
}

func TestRuntimeCalculator_CalculateNodeRuntimeDataTransfer(t *testing.T) {
	fromInstanceName := "instance1"
	fromRegionName := "region1"
	// Setup mock data
	defer monkey.UnpatchAll()
	performanceLoader, _ := loaders.NewPerformanceLoader(map[string]interface{}{})
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, "", performanceLoader)
	instanceIndexer := models.NewInstanceIndexer([]interface{}{})
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetRuntimeDistribution", func(wl *loaders.WorkflowLoader, instanceName string, regionName string, isRedirector bool) []float64 {
		return []float64{5.0, 5.1, 5.2}
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetAuxiliaryIndexTranslation", func(wl *loaders.WorkflowLoader, instanceName string, isRedirector bool) map[string]int {
		return map[string]int{
			"data_transfer_during_execution_gb": 0,
			"successor_instance":                1,
		}
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetAuxiliaryDataDistribution", func(wl *loaders.WorkflowLoader, instanceName string, regionName string, runtime float64, isRedirector bool) [][]float64 {
		return [][]float64{{0.1, 0.2}, {0.2, 0.3}}
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(performanceLoader), "GetRelativePerformance", func(pl *loaders.PerformanceLoader, regionName string) float64 {
		if regionName == fromRegionName {
			return 1.0
		} else {
			return 9.0
		}
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(instanceIndexer), "ValueToIndex", func(ix *models.InstanceIndexer, value string) int {
		if value == "successor_instance" {
			return 1
		} else {
			return 0
		}
	})
	monkey.Patch(rand.Int, func() int { return 0 })

	runtimeCalculator, _ := NewRuntimeCalculator(performanceLoader, workflowLoader)

	// Call the method
	runtimeData,
		currentExecutionTime,
		dataTransfer := runtimeCalculator.CalculateNodeRuntimeDataTransfer(
		fromInstanceName,
		fromRegionName,
		0.0,
		instanceIndexer,
		false,
	)

	// Verify results
	assert.Equal(t, runtimeData["current"], 5.0)
	assert.Equal(t, currentExecutionTime, 5.0)
	assert.Equal(t, dataTransfer, 0.1)
}

func TestRuntimeCalculator_GetTransmissionSizeDistribution(t *testing.T) {
	runtimeCalculator, _ := NewRuntimeCalculator(nil, nil)
	fromInstanceName := "instance1"
	toInstanceName := "instance2"
	runtimeCalculator.transmissionSizeDistributionCache = map[string][]float64{
		fmt.Sprintf("%s-%s", fromInstanceName, toInstanceName): {0.1, 0.2, 0.3},
	}
	expexted := []float64{0.1, 0.2, 0.3}
	sizeDistribution := runtimeCalculator.GetTransmissionSizeDistribution(fromInstanceName, toInstanceName)
	for i, size := range sizeDistribution {
		assert.Equal(t, expexted[i], size)
	}
}

func TestRuntimeCalculator_GetTransmissionLatencyDistribution(t *testing.T) {
	fromInstanceName := "instance1"
	toInstanceName := "instance2"
	fromRegionName := "region1"
	toRegionName := "region2"
	dataTransferSize := 1.0
	runtimeCalculator, _ := NewRuntimeCalculator(nil, nil)
	runtimeCalculator.transmissionLatencyDistributionCache = map[string][]float64{
		fmt.Sprintf("%s-%s", fromInstanceName, toInstanceName): {0.1, 0.2, 0.3},
		fmt.Sprintf("%v-%v-%v-%v-%v", fromInstanceName, toInstanceName, fromRegionName, toRegionName, dataTransferSize): {
			0.1,
			0.2,
			0.3,
		},
	}
	expexted := []float64{0.1, 0.2, 0.3}
	latencyDistribution := runtimeCalculator.GetTransmissionLatencyDistribution(
		fromInstanceName,
		fromRegionName,
		toInstanceName,
		toRegionName,
		dataTransferSize,
		false,
		false,
	)
	for i, latency := range latencyDistribution {
		assert.Equal(t, expexted[i], latency)
	}
}

func TestRuntimeCalculator_HandleMissingTransmissionLatencyDistribution(t *testing.T) {
	defer monkey.UnpatchAll()
	performanceLoader, _ := loaders.NewPerformanceLoader(map[string]interface{}{})
	monkey.PatchInstanceMethod(reflect.TypeOf(performanceLoader), "GetTransmissionLatencyDistribution", func(pl *loaders.PerformanceLoader, fromRegionName string, toRegionName string) []float64 {
		return []float64{0.2, 0.3, 0.4}
	})
	fromInstanceName := "instance1"
	toInstanceName := "instance2"
	fromRegionName := "region1"
	toRegionName := "region2"
	dataTransferSize := 1.0
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, fromRegionName, nil)
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetLatencyDistribution", func(wl *loaders.WorkflowLoader, fromI string, toI string, fromR string, toR string, size float64) []float64 {
		return []float64{0.5, 0.6, 0.7}
	})
	runtimeCalculator, _ := NewRuntimeCalculator(performanceLoader, workflowLoader)
	missingDistribution := runtimeCalculator.HandleMissingTransmissionLatencyDistribution(
		fromInstanceName,
		fromRegionName,
		toInstanceName,
		toRegionName,
		dataTransferSize,
		false,
	)
	expected := []float64{0.5, 0.6, 0.7}
	for i, d := range missingDistribution {
		assert.Equal(t, expected[i], d)
	}
}

func TestRuntimeCalculator_HandleMissingStartHopLatencyDistribution(t *testing.T) {
	// Mock the loader methods
	defer monkey.UnpatchAll()
	fromRegionName := "region1"
	toRegionName := "region2"
	dataTransferSize := 1.0
	performanceLoader, _ := loaders.NewPerformanceLoader(map[string]interface{}{})
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, fromRegionName, performanceLoader)
	monkey.PatchInstanceMethod(reflect.TypeOf(workflowLoader), "GetStartHopLatencyDistribution", func(wl *loaders.WorkflowLoader, toRegionName string, dataTransferSize float64) []float64 {
		return []float64{0.3, 0.4, 0.5}
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(performanceLoader), "GetTransmissionLatencyDistribution", func(pl *loaders.PerformanceLoader, fromRegionName string, toRegionName string) []float64 {
		return []float64{0.2, 0.3, 0.4}
	})

	runtimeCalculator, _ := NewRuntimeCalculator(performanceLoader, workflowLoader)

	// Call the method
	startHopDistribution := runtimeCalculator.HandleMissingStartHopLatencyDistribution(
		toRegionName, dataTransferSize,
	)

	expected := []float64{0.5, 0.7, 0.9}

	// Verify results
	for i, d := range startHopDistribution {
		assert.Equal(t, d, expected[i])
	}
}

func TestRuntimeCalculator_HandleMissingStartHopLatencyDistribution_HomeRegion(t *testing.T) {
	// Mock the loader methods
	toRegionName := "region2"
	dataTransferSize := 1.0
	workflowLoader, _ := loaders.NewWorkflowLoader(map[string]interface{}{}, map[string]interface{}{}, toRegionName, nil)
	runtimeCalculator, _ := NewRuntimeCalculator(nil, workflowLoader)

	// Expected panic if the home region has no latency data
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("HandleMissingStartHopLatencyDistribution Did not fail!")
		}
	}()
	runtimeCalculator.HandleMissingStartHopLatencyDistribution(toRegionName, dataTransferSize)
}
