package models

import (
	"reflect"
	"testing"

	"bou.ke/monkey"
	deploymentinput "caribou-go/src/deployment-input"
	"github.com/stretchr/testify/assert"
)

func TestSimulatedInstanceEdge_GetSimulatedTransmissionInformation_InvokedNode(t *testing.T) {
	defer monkey.UnpatchAll()
	mockFromNode := InstanceNode{invoked: true, actualInstanceId: 1, regionId: 10}
	monkey.PatchInstanceMethod(reflect.TypeOf(&mockFromNode), "GetCumulativeRuntime", func(node *InstanceNode, successorInstanceIndex int) float64 {
		return 15.0
	})
	mockToNode := InstanceNode{actualInstanceId: 2, regionId: 20}
	mockInputManager := deploymentinput.InputManager{}
	monkey.PatchInstanceMethod(reflect.TypeOf(&mockInputManager), "GetSimulatedTransmissionInfo", func(im *deploymentinput.InputManager,
		fromInstanceIndex int,
		uninvokedInstanceIndex int,
		simulatedSyncPredecessorIndex int,
		syncNodeIndex int,
		fromRegionIndex int,
		toRegionIndex int,
		cumulativeRuntime float64,
	) map[string]interface{} {
		assert.Equal(t, fromInstanceIndex, 1)
		assert.Equal(t, uninvokedInstanceIndex, 5)
		assert.Equal(t, simulatedSyncPredecessorIndex, 7)
		assert.Equal(t, syncNodeIndex, 2)
		assert.Equal(t, fromRegionIndex, 10)
		assert.Equal(t, toRegionIndex, 20)
		assert.Equal(t, cumulativeRuntime, 15.0)
		return map[string]interface{}{"latency": 0.1, "cost": 1.5, "carbon": 0.05}
	})
	sime := NewSimulatedInstanceEdge(
		&mockInputManager, &mockFromNode, &mockToNode, 5, 7,
	)
	result := sime.GetSimulatedTransmissionInformation()
	assert.Equal(t, result["latency"], 0.1)
	assert.Equal(t, result["cost"], 1.5)
	assert.Equal(t, result["carbon"], 0.05)
}

func TestSimulatedInstanceEdge_GetSimulatedTransmissionInformation_UninvokedNode(t *testing.T) {
	defer monkey.UnpatchAll()
	mockFromNode := InstanceNode{actualInstanceId: 1, regionId: 10}
	mockToNode := InstanceNode{actualInstanceId: 2, regionId: 20}
	mockInputManager := deploymentinput.InputManager{}
	sime := NewSimulatedInstanceEdge(
		&mockInputManager, &mockFromNode, &mockToNode, 5, 7,
	)
	result := sime.GetSimulatedTransmissionInformation()
	assert.Equal(t, len(result), 0)
}

func TestSimulatedInstanceEdge_GetSimulatedTransmissionInformation_RuntimeFromParent(t *testing.T) {
	defer monkey.UnpatchAll()
	mockFromNode := InstanceNode{invoked: true, actualInstanceId: 1, regionId: 10}
	monkey.PatchInstanceMethod(reflect.TypeOf(&mockFromNode), "GetCumulativeRuntime", func(node *InstanceNode, successorInstanceIndex int) float64 {
		return 30.0
	})
	mockToNode := InstanceNode{actualInstanceId: 2, regionId: 20}
	mockInputManager := deploymentinput.InputManager{}
	monkey.PatchInstanceMethod(reflect.TypeOf(&mockInputManager), "GetSimulatedTransmissionInfo", func(im *deploymentinput.InputManager,
		fromInstanceIndex int,
		uninvokedInstanceIndex int,
		simulatedSyncPredecessorIndex int,
		syncNodeIndex int,
		fromRegionIndex int,
		toRegionIndex int,
		cumulativeRuntime float64,
	) map[string]interface{} {
		assert.Equal(t, fromInstanceIndex, 1)
		assert.Equal(t, uninvokedInstanceIndex, 5)
		assert.Equal(t, simulatedSyncPredecessorIndex, 7)
		assert.Equal(t, syncNodeIndex, 2)
		assert.Equal(t, fromRegionIndex, 10)
		assert.Equal(t, toRegionIndex, 20)
		assert.Equal(t, cumulativeRuntime, 30.0)
		return map[string]interface{}{"latency": 0.2, "cost": 2.0, "carbon": 0.08}
	})
	sime := NewSimulatedInstanceEdge(
		&mockInputManager, &mockFromNode, &mockToNode, 5, 7,
	)
	result := sime.GetSimulatedTransmissionInformation()
	assert.Equal(t, result["latency"], 0.2)
	assert.Equal(t, result["cost"], 2.0)
	assert.Equal(t, result["carbon"], 0.08)
}
