package models

import (
	"reflect"
	"testing"

	"bou.ke/monkey"
	deploymentinput "caribou-go/src/deployment-input"
	"github.com/stretchr/testify/assert"
)

func TestInstanceEdge_GetTransmissionInformation_EdgeNotReal(t *testing.T) {
	defer monkey.UnpatchAll()
	mockFromNode := InstanceNode{invoked: false, regionId: 0, actualInstanceId: 1}
	mockToNode := InstanceNode{regionId: 1, actualInstanceId: 2}
	instanceEdge := NewInstanceEdge(nil, &mockFromNode, &mockToNode)
	result := instanceEdge.GetTransmissionInformation(false, false)
	assert.Equal(t, len(result), 0)
}

func TestInstanceEdge_GetTransmissionInformation_ConditionallyInvoked(t *testing.T) {
	defer monkey.UnpatchAll()
	mockFromNode := InstanceNode{invoked: true, regionId: 0, actualInstanceId: 1}
	mockToNode := InstanceNode{regionId: 1, actualInstanceId: 2}
	mockInputManager := deploymentinput.InputManager{}
	instanceEdge := NewInstanceEdge(&mockInputManager, &mockFromNode, &mockToNode)
	instanceEdge.conditionallyInvoked = true
	monkey.PatchInstanceMethod(reflect.TypeOf(&mockFromNode), "GetCumulativeRuntime", func(node *InstanceNode, successorInstanceIndex int) float64 {
		return 10.0
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(&mockInputManager), "GetTransmissionInfo", func(im *deploymentinput.InputManager, fromInstanceIndex int,
		fromRegionIndex int,
		toInstanceIndex int,
		toRegionIndex int,
		cumulativeRuntime float64,
		toInstanceIsSyncNode bool,
		considerFromClientLatency bool,
	) map[string]interface{} {
		assert.Equal(t, fromInstanceIndex, mockFromNode.actualInstanceId)
		assert.Equal(t, fromRegionIndex, mockFromNode.regionId)
		assert.Equal(t, toInstanceIndex, mockToNode.actualInstanceId)
		assert.Equal(t, toRegionIndex, mockToNode.regionId)
		assert.Equal(t, cumulativeRuntime, 10.0)
		assert.Equal(t, toInstanceIsSyncNode, true)
		assert.Equal(t, considerFromClientLatency, true)
		return map[string]interface{}{"latency": 1.0, "bandwidth": 100.0}
	})
	result := instanceEdge.GetTransmissionInformation(true, true)
	assert.Equal(t, result, map[string]interface{}{"latency": 1.0, "bandwidth": 100.0})
}

func TestInstanceEdge_GetTransmissionInformation_NotConditionallyInvoked(t *testing.T) {
	defer monkey.UnpatchAll()
	mockFromNode := InstanceNode{invoked: true, regionId: 0, actualInstanceId: 1}
	mockToNode := InstanceNode{regionId: 1, actualInstanceId: 2}
	mockInputManager := deploymentinput.InputManager{}
	instanceEdge := NewInstanceEdge(&mockInputManager, &mockFromNode, &mockToNode)
	monkey.PatchInstanceMethod(reflect.TypeOf(&mockInputManager), "GetNonExecutionInfo", func(im *deploymentinput.InputManager, fromInstanceIndex int,
		toInstanceIndex int,
	) map[string]interface{} {
		return map[string]interface{}{"status": "not_executed"}
	})
	result := instanceEdge.GetTransmissionInformation(false, false)
	assert.Equal(t, result, map[string]interface{}{"status": "not_executed"})
}
