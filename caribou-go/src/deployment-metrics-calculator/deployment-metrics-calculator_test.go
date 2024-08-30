package deployment_metrics_calculator

import (
	"reflect"
	"testing"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"
)

func TestSimpleDeploymentMetricsCalculator_PerformMonteCarloSimulation(t *testing.T) {
	defer monkey.UnpatchAll()
	dc := DeploymentMetricsCalculator{
		inputManager:                      nil,
		tailLatencyThreshold:              0,
		prerequisitesDictionary:           nil,
		successorDictionary:               nil,
		topologicalOrder:                  nil,
		homeRegionIndex:                   0,
		recordTransmissionExecutionCarbon: false,
		considerFromClientLatency:         false,
	}
	sdc := SimpleDeploymentMetricsCalculator{dc, 200}
	callCounter := 0
	monkey.PatchInstanceMethod(reflect.TypeOf(&dc), "CalculateWorkflow", func(dc *DeploymentMetricsCalculator, deployment []int) map[string]float64 {
		callCounter++
		return map[string]float64{"cost": 1.0, "runtime": 1.0, "carbon": 1.0}
	})
	result := sdc.PerformMonteCarloSimulation([]int{0, 1, 2, 3})
	assert.Equal(t, result["average_cost"], 1.0)
	assert.Equal(t, result["average_runtime"], 1.0)
	assert.Equal(t, result["average_carbon"], 1.0)
	assert.Equal(t, result["tail_cost"], 1.0)
	assert.Equal(t, result["tail_runtime"], 1.0)
	assert.Equal(t, result["tail_carbon"], 1.0)
	assert.Equal(t, callCounter, 20000)
}
