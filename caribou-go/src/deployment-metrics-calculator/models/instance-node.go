package models

import (
	deploymentinput "caribou-go/src/deployment-input"
)

type InstanceNode struct {
	inputManager                 *deploymentinput.InputManager
	regionId                     int
	nominalInstanceId            int
	actualInstanceId             int
	invoked                      bool
	trackedDataInputSizes        map[int]float64
	trackedDataOutputSizes       map[int]float64
	dataTransferDuringExecution  float64
	snsDataCallOutputSizes       map[int][]float64
	trackedDynamoDbWriteCapacity float64
	trackedDynamoDbReadCapacity  float64
	cumulativeRuntime            map[string]interface{}
	executionTime                float64
	isRedirector                 bool
}

func NewInstanceNode(im *deploymentinput.InputManager, instanceId int) *InstanceNode {
	actualId := -1
	if instanceId >= 0 {
		actualId = instanceId
	}
	return &InstanceNode{
		inputManager:                 im,
		regionId:                     -1,
		nominalInstanceId:            instanceId,
		actualInstanceId:             actualId,
		invoked:                      false,
		trackedDataInputSizes:        make(map[int]float64),
		trackedDataOutputSizes:       make(map[int]float64),
		dataTransferDuringExecution:  0.0,
		snsDataCallOutputSizes:       make(map[int][]float64),
		trackedDynamoDbReadCapacity:  0.0,
		trackedDynamoDbWriteCapacity: 0.0,
		cumulativeRuntime: map[string]interface{}{
			"current":    0.0,
			"successors": map[int]float64{},
		},
		executionTime: 0.0,
		isRedirector:  instanceId == -1,
	}
}

func (node *InstanceNode) GetCumulativeRuntime(successorInstanceIndex int) float64 {
	// Get the cumulative runtime of the successor edge
	// If there are no specifiec runtime for the successor
	// then return the current runtime of the node (Worse case scenario)
	if val, exists := node.cumulativeRuntime["successors"].(map[int]float64)[successorInstanceIndex]; exists {
		return val
	} else {
		return node.cumulativeRuntime["current"].(float64)
	}
}

func (node *InstanceNode) CalculateCarbonCostRuntime() map[string]float64 {
	// Calculate the cost and carbon of the node
	// based on the input/output size and dynamodb
	// read/write capacity
	// print(f'Calculating cost and carbon for node: {self.instance_id}')
	// print(f"cumulative_runtimes: {self.cumulative_runtimes}")
	var calculatedMetrics map[string]float64
	if node.actualInstanceId == -1 {
		calculatedMetrics = node.inputManager.CalculateCostCarbonVirtualStartInstance(
			node.trackedDataInputSizes,
			node.trackedDataOutputSizes,
			node.snsDataCallOutputSizes,
			node.trackedDynamoDbReadCapacity,
			node.trackedDynamoDbWriteCapacity,
		)
	} else {
		calculatedMetrics = node.inputManager.CalculateCostCarbonOfInstance(
			node.executionTime,
			node.actualInstanceId,
			node.regionId,
			node.trackedDataInputSizes,
			node.trackedDataOutputSizes,
			node.snsDataCallOutputSizes,
			node.dataTransferDuringExecution,
			node.trackedDynamoDbReadCapacity,
			node.trackedDynamoDbWriteCapacity,
			node.invoked,
			node.isRedirector,
		)
	}
	// print(calculated_metrics)
	// We only care about the runtime if the node was invoked
	runtime := 0.0
	if node.invoked {
		runtime = node.cumulativeRuntime["current"].(float64)
	}
	return map[string]float64{
		"cost":                calculatedMetrics["cost"],
		"runtime":             runtime,
		"execution_carbon":    calculatedMetrics["execution_carbon"],
		"transmission_carbon": calculatedMetrics["transmission_carbon"],
	}
}
