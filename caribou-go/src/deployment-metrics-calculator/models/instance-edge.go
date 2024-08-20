package models

import (
	deploymentinput "caribou-go/src/deployment-input"
)

type InstanceEdge struct {
	inputManager         *deploymentinput.InputManager
	fromInstanceNode     *InstanceNode
	toInstanceNode       *InstanceNode
	conditionallyInvoked bool
}

func NewInstanceEdge(inputManager *deploymentinput.InputManager, fromInstanceNode *InstanceNode, toInstanceNode *InstanceNode) *InstanceEdge {
	return &InstanceEdge{
		inputManager:         inputManager,
		fromInstanceNode:     fromInstanceNode,
		toInstanceNode:       toInstanceNode,
		conditionallyInvoked: false,
	}
}

func (edge *InstanceEdge) GetTransmissionInformation(successorIsSyncNode bool, considerFromClientLatency bool) map[string]interface{} {
	// Check the edge if it is a real edge
	// First get the parent node
	// If the parent node is invoked, then the edge is real
	// If the parent node is not invoked, then the edge is not real
	// If the edge is NOT real, then we should return None
	if edge.fromInstanceNode != nil && !edge.fromInstanceNode.invoked {
		return nil
	}
	fromInstanceId := edge.fromInstanceNode.actualInstanceId
	fromRegionId := edge.fromInstanceNode.regionId
	toInstanceId := edge.toInstanceNode.actualInstanceId
	toRegionId := edge.toInstanceNode.regionId

	var transmissionInfo map[string]interface{}
	// Those edges are actually apart of the workflow
	if edge.conditionallyInvoked {
		// This is the case where the edge is invoked conditionally
		cumulativeRuntime := 0.0
		// For non-starting edges, we should add the cumulative runtime of the parent node
		if edge.fromInstanceNode != nil {
			cumulativeRuntime += edge.fromInstanceNode.GetCumulativeRuntime(toInstanceId)
		}
		transmissionInfo = edge.inputManager.GetTransmissionInfo(
			fromInstanceId,
			fromRegionId,
			toInstanceId,
			toRegionId,
			cumulativeRuntime,
			successorIsSyncNode,
			considerFromClientLatency,
		)
	} else {
		transmissionInfo = edge.inputManager.GetNonExecutionInfo(fromInstanceId, toInstanceId)
	}
	return transmissionInfo
}
