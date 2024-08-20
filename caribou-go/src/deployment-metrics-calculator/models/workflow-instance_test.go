package models

import (
	"reflect"
	"testing"

	"bou.ke/monkey"
	deploymentinput "caribou-go/src/deployment-input"
	"github.com/stretchr/testify/assert"
)

func MockInputManager(wpdProbability float64) *deploymentinput.InputManager {
	im := deploymentinput.InputManager{}
	monkey.PatchInstanceMethod(reflect.TypeOf(&im), "GetStartHopRetrieveWpdProbability", func(im *deploymentinput.InputManager) float64 {
		return wpdProbability
	})
	return &im
}

func TestNewWorkflowInstance_ConfigureNodeRegions(t *testing.T) {
	defer monkey.UnpatchAll()
	im := MockInputManager(0.0)
	wi := NewWorkflowInstance(im, []int{0, 1, 2}, 0, true)
	wi.ConfigureNodeRegions([]int{0, 1, 2})
	assert.Equal(t, len(wi.nodes), 4)
}

func TestWorkflowInstance_AddEdge(t *testing.T) {
	defer monkey.UnpatchAll()
	im := MockInputManager(0.0)
	wi := NewWorkflowInstance(im, []int{0, 1, 2}, 0, true)
	wi.AddEdge(0, 1, true)
	_, ok := wi.edges[1][0]
	assert.True(t, ok)
}

func TestWorkflowInstance_AddNode(t *testing.T) {
	defer monkey.UnpatchAll()
	im := MockInputManager(0.0)
	wi := NewWorkflowInstance(im, []int{0, 1, 2}, 0, true)
	_ = wi.AddNode(1)
	_, ok := wi.nodes[1]
	assert.True(t, ok)
}
