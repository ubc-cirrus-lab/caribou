package models

import deploymentinput "caribou-go/src/deployment-input"

type SimulatedInstanceEdge struct {
	inputManager                       *deploymentinput.InputManager
	fromInstanceNode                   *InstanceNode
	toInstanceNode                     *InstanceNode
	unInvokedInstanceId                int
	simulatedSyncPredecessorInstanceId int
}

func NewSimulatedInstanceEdge(
	inputManager *deploymentinput.InputManager, fromInstanceNode *InstanceNode,
	toInstanceNode *InstanceNode, unInvokedInstanceId int,
	simulatedSyncPredecessorInstanceId int,
) *SimulatedInstanceEdge {
	return &SimulatedInstanceEdge{
		inputManager:                       inputManager,
		fromInstanceNode:                   fromInstanceNode,
		toInstanceNode:                     toInstanceNode,
		unInvokedInstanceId:                unInvokedInstanceId,
		simulatedSyncPredecessorInstanceId: simulatedSyncPredecessorInstanceId,
	}
}

func (e *SimulatedInstanceEdge) GetSimulatedTransmissionInformation() map[string]interface{} {
	// Check the edge if it is a simulated edge
	// If the edge is simulated, then we should return the transmission information
	// If the edge is NOT simulated, then we should return None
	if (e.fromInstanceNode != nil && !e.fromInstanceNode.invoked) || e.toInstanceNode == nil {
		return nil
	}

	fromInstanceId := e.fromInstanceNode.actualInstanceId
	uninvokedInstanceId := e.unInvokedInstanceId
	simulatedSyncPredecessorId := e.simulatedSyncPredecessorInstanceId
	syncNodeId := e.toInstanceNode.actualInstanceId

	fromRegionId := e.fromInstanceNode.regionId
	toRegionId := e.toInstanceNode.regionId

	cumulativeRuntime := 0.0
	// For non-starting edges, we should add the cumulative runtime of the parent node
	if e.fromInstanceNode != nil {
		// The time to call successor node is actually the cumulative time of the
		// parent node calling the uninvoked node
		cumulativeRuntime += e.fromInstanceNode.GetCumulativeRuntime(uninvokedInstanceId)
	}

	// print(f"SIE: FI: {from_instance_id}, UII: {uninvoked_instance_id}, Cumulative Runtime: {cumulative_runtime} s")

	// Those edges are not apart of the workflow
	// and are only used to handle latencies of non-execution of ancestor nodes
	transmissionInfo := e.inputManager.GetSimulatedTransmissionInfo(
		fromInstanceId,
		uninvokedInstanceId,
		simulatedSyncPredecessorId,
		syncNodeId,
		fromRegionId,
		toRegionId,
		cumulativeRuntime,
	)

	return transmissionInfo
}
