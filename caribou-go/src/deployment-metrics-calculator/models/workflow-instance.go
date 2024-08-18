package models

import (
	"log"
	"math/rand"
	"sort"

	deploymentinput "caribou-go/src/deployment-input"
)

type WorkflowInstance struct {
	inputManager              *deploymentinput.InputManager
	considerFromClientLatency bool
	startHopInstanceId        int
	hasRetrievedWpdFunction   bool
	redirectorExists          bool
	nodes                     map[int]*InstanceNode
	edges                     map[int]map[int]*InstanceEdge
	simulatedEdges            map[int]map[int]*SimulatedInstanceEdge
}

func NewWorkflowInstance(im *deploymentinput.InputManager, instanceDeploymentRegions []int, startHopInstanceIndex int, considerFromClientLatency bool) *WorkflowInstance {
	hasRetrievedWpdFunction := rand.Float64() < im.GetStartHopRetrieveWpdProbability()
	wi := &WorkflowInstance{
		inputManager:              im,
		considerFromClientLatency: considerFromClientLatency,
		startHopInstanceId:        startHopInstanceIndex,
		hasRetrievedWpdFunction:   hasRetrievedWpdFunction,
		redirectorExists:          hasRetrievedWpdFunction && instanceDeploymentRegions[startHopInstanceIndex] != im.GetHomeRegionIndex(),
		nodes:                     make(map[int]*InstanceNode),
		edges:                     make(map[int]map[int]*InstanceEdge),
		simulatedEdges:            make(map[int]map[int]*SimulatedInstanceEdge),
	}
	wi.ConfigureNodeRegions(instanceDeploymentRegions)
	return wi
}

func (wi *WorkflowInstance) ConfigureNodeRegions(instanceDeploymentRegions []int) {
	var virtualClientNode *InstanceNode
	if wi.redirectorExists {
		virtualClientNode = wi.getNode(-2)
		virtualClientNode.regionId = -1 // From unknown region
		// Create the redirector function node, which is present in the home region.
		redirectorNode := wi.getNode(-1)
		redirectorNode.regionId = wi.inputManager.GetHomeRegionIndex()
		redirectorNode.actualInstanceId = wi.startHopInstanceId
	} else {
		virtualClientNode = wi.getNode(-1)
		virtualClientNode.regionId = -1 // From unknown region
	}
	for instanceIndex, regionIndex := range instanceDeploymentRegions {
		currentNode := wi.getNode(instanceIndex)
		currentNode.regionId = regionIndex
	}
}

func (wi *WorkflowInstance) AddStartHop(startingInstanceIndex int) {
	var wpdRetrievalNode *InstanceNode
	var virtualClientNode *InstanceNode
	var redirectorNode *InstanceNode
	startHopNode := wi.getNode(startingInstanceIndex)
	if !wi.hasRetrievedWpdFunction {
		// If wpd is not retrieved at the function, then it will always be retrieved
		// in the virtual start hop. (Indicating client invocation region)
		// Also we will NEVER have a redirector as the request will always be sent
		// to the desired region.
		wpdRetrievalNode = wi.getNode(-1)
		virtualClientNode = wpdRetrievalNode
	} else {
		if wi.redirectorExists {
			// If the redirector exists, and retrieve WPD then we indicate
			// that the redirector function is invoked.
			wpdRetrievalNode = wi.getNode(-1)
			redirectorNode = wpdRetrievalNode
			virtualClientNode = wi.getNode(-2)
		} else {
			// Redirector does not exist, wpd is pulled at the start hop instance
			wpdRetrievalNode = startHopNode

			// There also exists a virtual client node
			virtualClientNode = wi.getNode(-1)
		}
	}

	// Set node invoked flags (Can be either virtual or real)
	// Note: redirector_node will always be wpd_retrieval_node if it exists
	wpdRetrievalNode.invoked = true
	virtualClientNode.invoked = true
	startHopNode.invoked = true

	// Create an new edge that goes from virtual client to the start hop
	// (or redirector if it exists)
	var virtualClientToFirstNodeEdge *InstanceEdge
	if redirectorNode != nil {
		virtualClientToFirstNodeEdge = wi.createEdge(
			virtualClientNode.nominalInstanceId, redirectorNode.nominalInstanceId,
		)
	} else {
		virtualClientToFirstNodeEdge = wi.createEdge(
			virtualClientNode.nominalInstanceId, startHopNode.nominalInstanceId,
		)
	}

	// It is impossible for the virtual client edge NOT to be invoked
	virtualClientToFirstNodeEdge.conditionallyInvoked = true

	// Get the WPD size of the starting node (This is the data that is downloaded from
	// system region to the wpd retrieval node)
	//// While data is also moved FROM system region, we do not need to track or model
	//// this data transfer as it is not part of the workflow

	startHopNodeInfo := wi.inputManager.GetStartHopInfo()
	dynamodbReadCapacityUnits := startHopNodeInfo["read_capacity_units"]
	wpdRetrievalNode.trackedDynamoDbReadCapacity += dynamodbReadCapacityUnits

	//// Deal with the download size of the starting node
	workflowPlacementDecisionSize := startHopNodeInfo["workflow_placement_decision_size"]

	//// Data is downloaded from the system region to the wpd_retrieval_node node
	//// We define -2 as the system region location.
	wi.manageDataTransferDict(
		wpdRetrievalNode.trackedDataInputSizes,
		-2,
		workflowPlacementDecisionSize,
	)

	// If the redirector exists, we need to handle the edge and node of it
	if redirectorNode != nil {
		// Handle edge
		wi.AddEdge(redirectorNode.nominalInstanceId, startHopNode.nominalInstanceId, true)

		// handle/materialize the node)
		wi.AddNode(redirectorNode.nominalInstanceId)
	}
}

func (wi *WorkflowInstance) AddEdge(fromInstanceIndex int, toInstanceIndex int, invoked bool) {
	// Get the from node
	// We need this to determine if the edge is actually
	// invoked or not, as if the from node is not invoked
	// then the edge is not invoked.
	fromNode := wi.getNode(fromInstanceIndex)

	// Get the edge (Create if not exists)
	currentEdge := wi.createEdge(fromInstanceIndex, toInstanceIndex)

	// A edge can only be conditionally invoked if BOTH the
	// previous node and the current edge is designated as invoked
	currentEdge.conditionallyInvoked = invoked && fromNode.invoked
}

func (wi *WorkflowInstance) AddNode(instanceIndex int) bool {
	/**
	Add a new node to the workflow instance.
		This function will also link all the edges that go to this node.
		And calculate and materialize the cumulative runtime of the node.
		And also the cost and carbon of the edge to the node.
		Return if the workflow instance was invoked.
	*/

	// Create a new node (This will always create a new node if
	// this class is configured correctly)
	nominalInstanceId := instanceIndex
	currentNode := wi.getNode(instanceIndex)
	actualInstanceId := currentNode.actualInstanceId

	// Look through all the real edges linking to this node
	nodeInvoked := false
	realPredecessorEdges := getPredecessorEdges(nominalInstanceId, wi.edges)

	isSyncNode := len(realPredecessorEdges) > 1
	syncEdgeUploadData := [][]float64{}
	edgeReachedTimeToSnsData := []struct {
		startTime float64
		snsData   map[string]interface{}
	}{}
	for _, currentEdge := range realPredecessorEdges {
		// Real edge are defined as edges that are actually apart of the workflow
		// This will also manage and determine if this current node actually is invoked
		// and also create virtual edges for the purposes of handling non-execution.
		currentNodeInvoked := wi.handleRealEdge(
			currentEdge, isSyncNode, &syncEdgeUploadData, &edgeReachedTimeToSnsData,
		)
		nodeInvoked = nodeInvoked || currentNodeInvoked

		//from_instance_id := -1
		//if current_edge.fromInstanceNode != nil {
		//	from_instance_id = current_edge.fromInstanceNode.instanceId
		//}
		// print(
		//     f"WI: Processing Real Edge, from: {from_instance_id} -> {current_edge.to_instance_node.instance_id} -> {current_node_invoked}"
		// )
	}

	// Handle sync upload auxiliary data
	//// This the write capacity unit of the sync node
	// Purely from data upload and download, where upload
	// is done via UpdateItem (Which consumes whole write
	// capacity unit of size of the table)
	if len(syncEdgeUploadData) > 0 {
		capacityUnits := wi.inputManager.CalculateDynamoDbCapacityUnitOfSyncEdges(syncEdgeUploadData)
		currentNode.trackedDynamoDbWriteCapacity += capacityUnits["write_capacity_units"]
		currentNode.trackedDynamoDbReadCapacity += capacityUnits["read_capacity_units"]
	}
	// Calculate the cumulative runtime of the  node
	// Only if the node was invoked
	if nodeInvoked {
		// We only care about simulated edges IFF the node was invoked
		// As it determines the actual runtime of the node (and represent SNS call)
		simulatedPredecessorEdges := getPredecessorEdges(nominalInstanceId, wi.simulatedEdges)
		for _, simulatedEdge := range simulatedPredecessorEdges {
			// print(
			//     f"WI: Processing Simulated Edge, from: {simulated_edge.from_instance_node.instance_id} -> {simulated_edge.to_instance_node.instance_id}"
			// )
			wi.handleSimulatedEdge(simulatedEdge, &edgeReachedTimeToSnsData)
		}

		// Calculate the cumulative runtime of the node and the data transfer during execution
		cumulativeRuntime := wi.handleSNSInvocation(&edgeReachedTimeToSnsData)
		// print(f"WI: Runtime before execution: {cumulative_runtime} s")
		currentRuntime, executionTime, dataTransferDuringExecution := wi.inputManager.GetNodeRuntimeDataTransfer(
			actualInstanceId, currentNode.regionId, cumulativeRuntime, currentNode.isRedirector,
		)
		currentNode.cumulativeRuntime = currentRuntime
		// print(f"WI: Runtimes after execution: {current_node.cumulative_runtimes} s")
		currentNode.executionTime = executionTime
		// Handle the data transfer during execution
		// We will asume the data comes from the same region as the node
		currentNode.dataTransferDuringExecution += dataTransferDuringExecution
	}
	// Set the node invoked flag
	currentNode.invoked = nodeInvoked

	return nodeInvoked
}

func (wi *WorkflowInstance) handleSNSInvocation(edgeReachedTimeToSnsData *[]struct {
	startTime float64
	snsData   map[string]interface{}
},
) float64 {
	sort.Slice(*edgeReachedTimeToSnsData, func(i, j int) bool {
		return (*edgeReachedTimeToSnsData)[i].startTime > (*edgeReachedTimeToSnsData)[j].startTime
	})
	if len(*edgeReachedTimeToSnsData) > 0 {
		// Get the edge that will invoke the SNS
		snsEdgeData := (*edgeReachedTimeToSnsData)[0].snsData
		cumulativeRuntime := snsEdgeData["cumulative_runtime"].(float64)
		snsDataTransferSize := snsEdgeData["sns_data_transfer_size"].(float64)
		fromInstanceNode := snsEdgeData["from_instance_node"].(*InstanceNode)
		toInstanceNode := snsEdgeData["to_instance_node"].(*InstanceNode)

		//// This means that the predecessor node should have the data output size incremented
		//// If the predecessor node exists (Aka not the start node)
		fromRegionId := fromInstanceNode.regionId
		toRegionId := toInstanceNode.regionId
		wi.manageDataTransferDict(
			fromInstanceNode.trackedDataOutputSizes, toRegionId, snsDataTransferSize,
		)
		// Data move from the predecessor node to the current node
		wi.manageDataTransferDict(
			toInstanceNode.trackedDataInputSizes, fromRegionId, snsDataTransferSize,
		)

		// Payload is uploaded to the SNS
		wi.manageSNSInvocationDataTransferDict(
			fromInstanceNode.snsDataCallOutputSizes, toRegionId, snsDataTransferSize,
		)

		return cumulativeRuntime
	}
	return 0.0
}

func (wi *WorkflowInstance) handleSimulatedEdge(
	currentEdge *SimulatedInstanceEdge,
	edgeReachedTimeToSnsData *[]struct {
		startTime float64
		snsData   map[string]interface{}
	},
) {
	// Get the transmission information of the edge
	transmissionInfo := currentEdge.GetSimulatedTransmissionInformation()
	startingRuntime := transmissionInfo["starting_runtime"].(float64)
	cumulativeRuntime := transmissionInfo["cumulative_runtime"].(float64)
	snsDataTransferSize := transmissionInfo["sns_data_transfer_size"].(float64)

	*edgeReachedTimeToSnsData = append(
		*edgeReachedTimeToSnsData,
		struct {
			startTime float64
			snsData   map[string]interface{}
		}{
			startTime: startingRuntime,
			snsData: map[string]interface{}{
				"from_instance_node":     currentEdge.fromInstanceNode,
				"to_instance_node":       currentEdge.toInstanceNode,
				"cumulative_runtime":     cumulativeRuntime,
				"sns_data_transfer_size": snsDataTransferSize,
			},
		},
	)
}

func (wi *WorkflowInstance) handleRealEdge(
	currentEdge *InstanceEdge,
	successorIsSyncNode bool,
	syncEdgeUploadData *[][]float64,
	edgeReachedTimeToSnsData *[]struct {
		startTime float64
		snsData   map[string]interface{}
	},
) bool {
	nodeInvoked := false
	transmissionInfo := currentEdge.GetTransmissionInformation(successorIsSyncNode, wi.considerFromClientLatency)
	if len(transmissionInfo) > 0 {
		if currentEdge.conditionallyInvoked {
			startingRuntime := transmissionInfo["starting_runtime"].(float64)
			cumulativeRuntime := transmissionInfo["cumulative_runtime"].(float64)
			snsDataTransferSize := transmissionInfo["sns_data_transfer_size"].(float64)

			*edgeReachedTimeToSnsData = append(
				*edgeReachedTimeToSnsData,
				struct {
					startTime float64
					snsData   map[string]interface{}
				}{
					startTime: startingRuntime,
					snsData: map[string]interface{}{
						"from_instance_node":     currentEdge.fromInstanceNode,
						"to_instance_node":       currentEdge.toInstanceNode,
						"cumulative_runtime":     cumulativeRuntime,
						"sns_data_transfer_size": snsDataTransferSize,
					},
				},
			)
			if successorIsSyncNode {
				if currentEdge.fromInstanceNode.nominalInstanceId == -1 {
					log.Fatalf("Sync node must have a predecessor that is not the start hop, destination instance: %v", currentEdge.toInstanceNode.nominalInstanceId)
				}
				syncInfo := transmissionInfo["sync_info"].(map[string]interface{})
				syncSize := syncInfo["sync_size"].(float64)
				consumedWcu := syncInfo["consumed_dynamodb_write_capacity_units"].(float64)
				dynamodbUploadSize := syncInfo["dynamodb_upload_size"].(float64)

				syncUploadAuxiliaryInfo := syncInfo["sync_upload_auxiliary_info"].([]float64)
				*syncEdgeUploadData = append(*syncEdgeUploadData, syncUploadAuxiliaryInfo)

				// Increment the consumed write capacity at the location
				// of the SYNC Node (Outgoing node, Current node)
				currentEdge.toInstanceNode.trackedDynamoDbWriteCapacity += consumedWcu

				// Data denoted by dynamodb_upload_size is uploaded to the sync table (Move out of the predecessor node, and into the current, sync node)
				wi.manageDataTransferDict(
					currentEdge.fromInstanceNode.trackedDataOutputSizes,
					currentEdge.toInstanceNode.regionId,
					dynamodbUploadSize,
				)
				wi.manageDataTransferDict(
					currentEdge.toInstanceNode.trackedDataInputSizes,
					currentEdge.fromInstanceNode.regionId,
					dynamodbUploadSize,
				)

				// Data moves from the location of the sync node to the predecessor node
				//// This means that the predecessor node should have the data input size incremented
				//// This means that the current node should have the data output size incremented
				wi.manageDataTransferDict(
					currentEdge.toInstanceNode.trackedDataOutputSizes,
					currentEdge.fromInstanceNode.regionId,
					syncSize,
				)
				wi.manageDataTransferDict(
					currentEdge.fromInstanceNode.trackedDataInputSizes,
					currentEdge.toInstanceNode.regionId,
					syncSize,
				)
			}

			// Mark that the node was invoked
			nodeInvoked = true
		} else {
			// Non-invoked node cannot be a virtual start node.
			if currentEdge.fromInstanceNode.nominalInstanceId == -1 {
				log.Fatalf(
					"Non-execution node must have a predecessor that is not the start hop!\ninstance: %v", currentEdge.toInstanceNode.nominalInstanceId,
				)
			}

			// For the non-execution case, we should get the instances that the sync node will write to
			// This will increment the consumed write capacity of the sync node, and also the data transfer size
			nonExecutionInfos := transmissionInfo["non_execution_info"].([]map[string]interface{})
			for _, nonExecutionInfo := range nonExecutionInfos {
				simulatedPredecessorInstanceId := nonExecutionInfo["predecessor_instance_id"].(int)
				syncNodeInstanceId := nonExecutionInfo["sync_node_instance_id"].(int)
				consumedWcu := nonExecutionInfo["consumed_dynamodb_write_capacity_units"].(float64)
				syncDataResponseSize := nonExecutionInfo["sync_size"].(float64)

				// Get the sync node
				//// Append the consumed write capacity to the sync node
				syncNode := wi.getNode(syncNodeInstanceId)
				syncNode.trackedDynamoDbWriteCapacity += consumedWcu

				// Data of sync_data_response_size is moved from the sync node to the current node
				// Where current node refers to the node calling the non-execution edge
				wi.manageDataTransferDict(
					syncNode.trackedDataOutputSizes,
					currentEdge.fromInstanceNode.regionId,
					syncDataResponseSize,
				)
				wi.manageDataTransferDict(
					currentEdge.fromInstanceNode.trackedDataInputSizes,
					syncNode.regionId,
					syncDataResponseSize,
				)

				// Add a simulated edge from the edge from node to the sync node
				wi.createSimulatedEdge(
					currentEdge.fromInstanceNode.nominalInstanceId,
					currentEdge.toInstanceNode.nominalInstanceId,
					simulatedPredecessorInstanceId,
					syncNodeInstanceId,
				)
			}
		}
	} else {
		realNode := currentEdge.fromInstanceNode.invoked
		if realNode {
			// In the case the edge is real, there should never be a case where
			// there are no transmission information
			log.Fatalf(
				"No transmission info in edge\nNo transmission info in edge\nNode: %v\nConditionally Invoked: %v\nTo: %v\n",
				currentEdge.toInstanceNode.nominalInstanceId,
				currentEdge.conditionallyInvoked,
				currentEdge.toInstanceNode.nominalInstanceId,
			)
		}
	}
	return nodeInvoked
}

func (wi *WorkflowInstance) CalculateOverallCostRuntimeCarbon() map[string]float64 {
	cumulativeCost := 0.0
	maxRuntime := 0.0
	cumulativeExecutionCarbon := 0.0
	cumulativeTransmissionCarbon := 0.0
	keys := []int{}
	for k := range wi.nodes {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		node := wi.nodes[k]
		nodeCarbonCostRuntime := node.CalculateCarbonCostRuntime()
		cumulativeCost += nodeCarbonCostRuntime["cost"]
		cumulativeExecutionCarbon += nodeCarbonCostRuntime["execution_carbon"]
		cumulativeTransmissionCarbon += nodeCarbonCostRuntime["transmission_carbon"]
		maxRuntime = max(maxRuntime, nodeCarbonCostRuntime["runtime"])
	}
	return map[string]float64{
		"cost":                cumulativeCost,
		"runtime":             maxRuntime,
		"carbon":              cumulativeExecutionCarbon + cumulativeTransmissionCarbon,
		"execution_carbon":    cumulativeExecutionCarbon,
		"transmission_carbon": cumulativeTransmissionCarbon,
	}
}

func (wi *WorkflowInstance) getNode(instanceIndex int) *InstanceNode {
	// Get node if exists, else create a new node
	if _, exists := wi.nodes[instanceIndex]; !exists {
		// Create new node
		node := NewInstanceNode(wi.inputManager, instanceIndex)
		wi.nodes[instanceIndex] = node
	}
	return wi.nodes[instanceIndex]
}

func (wi *WorkflowInstance) createEdge(fromInstanceIndex int, toInstanceIndex int) *InstanceEdge {
	// Create a new edge (And specify the from and to nodes)
	edge := NewInstanceEdge(wi.inputManager, wi.getNode(fromInstanceIndex), wi.getNode(toInstanceIndex))

	// Add the edge to the edge dictionary
	if _, exists := wi.edges[toInstanceIndex]; !exists {
		wi.edges[toInstanceIndex] = make(map[int]*InstanceEdge)
	}

	wi.edges[toInstanceIndex][fromInstanceIndex] = edge

	return edge
}

func (wi *WorkflowInstance) createSimulatedEdge(
	fromInstanceId int, uninvokedInstanceId int, simulatedSyncPredecessorId int, syncNodeId int,
) *SimulatedInstanceEdge {
	simulatedEdge := NewSimulatedInstanceEdge(
		wi.inputManager,
		wi.getNode(fromInstanceId),
		wi.getNode(syncNodeId),
		uninvokedInstanceId,
		simulatedSyncPredecessorId,
	)

	if _, exists := wi.simulatedEdges[syncNodeId]; !exists {
		wi.simulatedEdges[syncNodeId] = map[int]*SimulatedInstanceEdge{}
	}
	wi.simulatedEdges[syncNodeId][fromInstanceId] = simulatedEdge
	return simulatedEdge
}

func (wi *WorkflowInstance) manageDataTransferDict(
	dataTransferDict map[int]float64, regionId int, dataTransferSize float64,
) {
	if _, exists := dataTransferDict[regionId]; !exists {
		dataTransferDict[regionId] = 0.0
	}
	dataTransferDict[regionId] += dataTransferSize
}

func (wi *WorkflowInstance) manageSNSInvocationDataTransferDict(
	snsDataTransferDict map[int][]float64, regionId int, dataTransferSize float64,
) {
	if _, exists := snsDataTransferDict[regionId]; !exists {
		snsDataTransferDict[regionId] = []float64{}
	}
	snsDataTransferDict[regionId] = append(snsDataTransferDict[regionId], dataTransferSize)
}

func getPredecessorEdges[T SimulatedInstanceEdge | InstanceEdge](instanceIndex int, edges map[int]map[int]*T) []*T {
	var predEdges []*T
	if edgesMap, exists := edges[instanceIndex]; exists {
		for _, e := range edgesMap {
			predEdges = append(predEdges, e)
		}
	}
	return predEdges
}
