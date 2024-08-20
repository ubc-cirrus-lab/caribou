package models

import (
	"reflect"
	"testing"

	"bou.ke/monkey"
	deploymentinput "caribou-go/src/deployment-input"
	"github.com/stretchr/testify/assert"
)

func TestInstanceNode_GetCumulativeRuntime_NoSuccessor(t *testing.T) {
	defer monkey.UnpatchAll()
	im := deploymentinput.InputManager{}
	inode := NewInstanceNode(&im, 1)
	runtime := inode.GetCumulativeRuntime(999)
	assert.Equal(t, runtime, 0.0)
}

func TestInstanceNode_GetCumulativeRuntime_WithSuccessor(t *testing.T) {
	defer monkey.UnpatchAll()
	im := deploymentinput.InputManager{}
	inode := NewInstanceNode(&im, 1)
	inode.cumulativeRuntime["successors"].(map[int]float64)[999] = 10.0
	runtime := inode.GetCumulativeRuntime(999)
	assert.Equal(t, runtime, 10.0)
}

func TestInstanceNode_CalculateCarbonCostRuntime_VirtualStartInstance(t *testing.T) {
	defer monkey.UnpatchAll()
	im := deploymentinput.InputManager{}
	inode := NewInstanceNode(&im, -1)
	inode.cumulativeRuntime["current"] = 15.0
	monkey.PatchInstanceMethod(reflect.TypeOf(&im), "CalculateCostCarbonVirtualStartInstance", func(im *deploymentinput.InputManager,
		dataInputSizes map[int]float64,
		dataOutputSizes map[int]float64,
		snsDataCallAndOutputSizes map[int][]float64,
		dynamodbReadCapacity float64,
		dynamodbWriteCapacity float64,
	) map[string]float64 {
		return map[string]float64{
			"cost":                200.0,
			"execution_carbon":    75.0,
			"transmission_carbon": 10.0,
		}
	})
	metrics := inode.CalculateCarbonCostRuntime()
	assert.Equal(t, metrics["cost"], 200.0)
	assert.Equal(t, metrics["runtime"], 0.0)
	assert.Equal(t, metrics["execution_carbon"], 75.0)
	assert.Equal(t, metrics["transmission_carbon"], 10.0)
}

func TestInstanceNode_CalculateCarbonCostRuntime_ActualInstance(t *testing.T) {
	defer monkey.UnpatchAll()
	im := deploymentinput.InputManager{}
	inode := NewInstanceNode(&im, 1)
	inode.invoked = true
	inode.cumulativeRuntime["current"] = 15.0
	monkey.PatchInstanceMethod(reflect.TypeOf(&im), "CalculateCostCarbonOfInstance", func(im *deploymentinput.InputManager,
		executionTime float64,
		instanceIndex int,
		regionIndex int,
		dataInputSizes map[int]float64,
		dataOutputSizes map[int]float64,
		snsDataCallAndOutputSizes map[int][]float64,
		dataTransferDuringExecution float64,
		dynamodbReadCapacity float64,
		dynamodbWriteCapacity float64,
		isInvoked bool,
		isRedirector bool,
	) map[string]float64 {
		return map[string]float64{
			"cost":                200.0,
			"execution_carbon":    75.0,
			"transmission_carbon": 20.0,
		}
	})
	metrics := inode.CalculateCarbonCostRuntime()
	assert.Equal(t, metrics["cost"], 200.0)
	assert.Equal(t, metrics["runtime"], 15.0)
	assert.Equal(t, metrics["execution_carbon"], 75.0)
	assert.Equal(t, metrics["transmission_carbon"], 20.0)
}

func TestInstanceNode_CalculateCarbonCostRuntime_NotInvoked(t *testing.T) {
	im := deploymentinput.InputManager{}
	inode := NewInstanceNode(&im, 1)
	monkey.PatchInstanceMethod(reflect.TypeOf(&im), "CalculateCostCarbonOfInstance", func(im *deploymentinput.InputManager,
		executionTime float64,
		instanceIndex int,
		regionIndex int,
		dataInputSizes map[int]float64,
		dataOutputSizes map[int]float64,
		snsDataCallAndOutputSizes map[int][]float64,
		dataTransferDuringExecution float64,
		dynamodbReadCapacity float64,
		dynamodbWriteCapacity float64,
		isInvoked bool,
		isRedirector bool,
	) map[string]float64 {
		return map[string]float64{
			"cost":                300.0,
			"execution_carbon":    125.0,
			"transmission_carbon": 30.0,
		}
	})
	metrics := inode.CalculateCarbonCostRuntime()
	assert.Equal(t, metrics["cost"], 300.0)
	assert.Equal(t, metrics["runtime"], 0.0)
	assert.Equal(t, metrics["execution_carbon"], 125.0)
	assert.Equal(t, metrics["transmission_carbon"], 30.0)
}
