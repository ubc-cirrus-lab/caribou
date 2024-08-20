package deployment_input

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"

	"caribou-go/src/deployment-input/components/calculators"
	"caribou-go/src/deployment-input/components/loaders"
	"caribou-go/src/models"
)

type InputManager struct {
	RegionViabilityLoader      *loaders.RegionViabilityLoader
	DataCenterLoader           *loaders.DataCenterLoader
	PerformanceLoader          *loaders.PerformanceLoader
	CarbonLoader               *loaders.CarbonLoader
	WorkflowLoader             *loaders.WorkflowLoader
	RuntimeCalculator          *calculators.RuntimeCalculator
	CarbonCalculator           *calculators.CarbonCalculator
	CostCalculator             *calculators.CostCalculator
	InstanceIndexer            *models.InstanceIndexer
	RegionIndexer              *models.RegionIndexer
	executionLatencyCache      map[string][]float64
	invocationProbabilityCache map[string]float64
}

func NewInputManager(
	RegionViabilityLoader *loaders.RegionViabilityLoader,
	DataCenterLoader *loaders.DataCenterLoader,
	PerformanceLoader *loaders.PerformanceLoader,
	CarbonLoader *loaders.CarbonLoader,
	WorkflowLoader *loaders.WorkflowLoader,
	RuntimeCalculator *calculators.RuntimeCalculator,
	CarbonCalculator *calculators.CarbonCalculator,
	CostCalculator *calculators.CostCalculator,
	InstanceIndexer *models.InstanceIndexer,
	RegionIndexer *models.RegionIndexer,
) (*InputManager, error) {
	return &InputManager{
		RegionViabilityLoader:      RegionViabilityLoader,
		DataCenterLoader:           DataCenterLoader,
		PerformanceLoader:          PerformanceLoader,
		CarbonLoader:               CarbonLoader,
		WorkflowLoader:             WorkflowLoader,
		RuntimeCalculator:          RuntimeCalculator,
		CarbonCalculator:           CarbonCalculator,
		CostCalculator:             CostCalculator,
		InstanceIndexer:            InstanceIndexer,
		RegionIndexer:              RegionIndexer,
		executionLatencyCache:      map[string][]float64{},
		invocationProbabilityCache: map[string]float64{},
	}, nil
}

func Setup(data map[string]interface{}) *InputManager {
	regionViabilityLoader, _ := loaders.NewRegionViabilityLoader(data["region_viability_loader"].([]interface{}))

	datacenterLoader, _ := loaders.NewDataCenterLoader(
		data["datacenter_loader"].(map[string]interface{}),
	)

	performanceLoader, _ := loaders.NewPerformanceLoader(
		data["performance_loader"].(map[string]interface{}),
	)

	carbonLoader, _ := loaders.NewCarbonLoader(
		data["carbon_loader"].(map[string]interface{}),
	)

	workflowLoader, _ := loaders.NewWorkflowLoader(
		data["workflow_loader"].(map[string]interface{})["workflow_data"].(map[string]interface{}),
		data["workflow_loader"].(map[string]interface{})["instances_regions_and_providers"].(map[string]interface{}),
		data["workflow_loader"].(map[string]interface{})["home_region"].(string),
		performanceLoader,
	)

	runtimeCalculator, _ := calculators.NewRuntimeCalculator(
		performanceLoader, workflowLoader,
	)

	carbonCalculator, _ := calculators.NewCarbonCalculator(
		carbonLoader,
		datacenterLoader,
		workflowLoader,
		data["consider_cfe"].(bool),
		data["energy_factor"].(float64),
		data["carbon_free_intra_region_transmission"].(bool),
		data["carbon_free_dt_during_execution_at_home_region"].(bool),
	)

	costCalculator, _ := calculators.NewCostCalculator(
		datacenterLoader, workflowLoader, runtimeCalculator,
	)

	instanceIndexer := models.CopyInstanceIndexer(data["instance_indexer"].(map[string]interface{}))
	regionIndexer := models.CopyRegionIndexer(data["region_indexer"].(map[string]interface{}))

	inputManager, _ := NewInputManager(
		regionViabilityLoader,
		datacenterLoader,
		performanceLoader,
		carbonLoader,
		workflowLoader,
		runtimeCalculator,
		carbonCalculator,
		costCalculator,
		instanceIndexer,
		regionIndexer,
	)

	fmt.Println("Go: Created Input Manager")

	return inputManager
}

func (im *InputManager) AlterCarbonSetting(setting *string) {
	im.CarbonCalculator.AlterCarbonSetting(setting)
	im.RuntimeCalculator.ResetCache()

	// Clear the cache
	im.executionLatencyCache = map[string][]float64{}
	im.invocationProbabilityCache = map[string]float64{}
}

func (im *InputManager) GetInvocationProbability(fromInstanceIndex int, toInstanceIndex int) float64 {
	// 	Return the probability of the edge being triggered.
	// Check if the value is already in the cache
	key := strconv.Itoa(fromInstanceIndex) + "_" + strconv.Itoa(toInstanceIndex)
	if val, exists := im.invocationProbabilityCache[key]; exists {
		return val
	}

	// Convert the instance indices to their names
	fromInstanceName := im.InstanceIndexer.IndexToValue(fromInstanceIndex)
	toInstanceName := im.InstanceIndexer.IndexToValue(toInstanceIndex)

	// If not, retrieve the value from the workflow loader
	invocationProbability := im.WorkflowLoader.GetInvocationProbability(fromInstanceName, toInstanceName)
	im.invocationProbabilityCache[key] = invocationProbability
	return invocationProbability
}

func (im *InputManager) GetStartHopRetrieveWpdProbability() float64 {
	// Return the probability of workflow placement decision being retrieved at
	// the first function (or redirector) rather than the client CLI.
	// If not, retrieve the value from the workflow loader
	return im.WorkflowLoader.GetStartHopRetrieveWpdProbability()
}

func (im *InputManager) GetAllRegions() []string {
	return im.RegionViabilityLoader.AvailableRegions
}

func (im *InputManager) GetTransmissionInfo(
	fromInstanceIndex int,
	fromRegionIndex int,
	toInstanceIndex int,
	toRegionIndex int,
	cumulativeRuntime float64,
	toInstanceIsSyncNode bool,
	considerFromClientLatency bool,
) map[string]interface{} {
	// Convert the instance and region indices to their names
	// For start hop, from_instance_index and from_region_index will be -1
	fromInstanceName := ""
	fromRegionName := ""
	if fromInstanceIndex != -1 {
		fromInstanceName = im.InstanceIndexer.IndexToValue(fromInstanceIndex)
	}
	if fromRegionIndex != -1 {
		fromRegionName = im.RegionIndexer.IndexToValue(fromRegionIndex)
	}
	// To instance and region will always have region and instance index
	toInstanceName := im.InstanceIndexer.IndexToValue(toInstanceIndex)
	toRegionName := im.RegionIndexer.IndexToValue(toRegionIndex)
	// Get a transmission size and latency sample
	transmissionSize, transmissionLatency := im.RuntimeCalculator.CalculateTransmissionSizeLatency(
		fromInstanceName, fromRegionName, toInstanceName, toRegionName, toInstanceIsSyncNode, considerFromClientLatency,
	)
	snsTransmissionSize := transmissionSize
	var syncInfo map[string]interface{}
	if toInstanceIsSyncNode {
		// If to instance is a sync node, then at the same time,
		// we can retrieve the sync_sizes_gb and sns_only_sizes_gb
		// And then calculate the sync node related information.
		if len(fromInstanceName) == 0 {
			log.Fatalf("Start hop cannot have a sync node as a successor")
		}
		snsOnlySize, syncSize, wcu := im.GetUploadSyncSizeAndWcu(fromInstanceName, toInstanceName)
		snsTransmissionSize = snsOnlySize
		syncInfo = map[string]interface{}{
			"dynamodb_upload_size":                   transmissionSize,
			"sync_size":                              syncSize,
			"consumed_dynamodb_write_capacity_units": wcu,
			"sync_upload_auxiliary_info":             []float64{cumulativeRuntime, transmissionSize},
		}
	}
	// If to instance is a sync node, then at the same time,
	// // we can retrieve the sync_sizes_gb and sns_only_sizes_gb
	// And then calculate the sync node related information.
	return map[string]interface{}{
		"starting_runtime":       cumulativeRuntime,
		"cumulative_runtime":     cumulativeRuntime + transmissionLatency,
		"sns_data_transfer_size": snsTransmissionSize,
		"sync_info":              syncInfo,
	}
}

func (im *InputManager) GetUploadSyncSizeAndWcu(
	fromInstanceName string, toInstanceName string,
) (float64, float64, float64) {
	// If to instance is a sync node, then at the same time,
	// we can retrieve the sync_sizes_gb and sns_only_sizes_gb
	snsOnlySize := im.WorkflowLoader.GetSNSOnlySize(fromInstanceName, toInstanceName)
	syncSize := im.WorkflowLoader.GetSyncSize(fromInstanceName, toInstanceName)

	// We have to get sync_size * 2, as our wrapper does 2 update operations
	dynamodbWriteCapacityUnits := im.CalculateWriteCapacity(syncSize) * 2

	return snsOnlySize, syncSize, dynamodbWriteCapacityUnits
}

func (im *InputManager) CalculateWriteCapacity(dataSizeGb float64) float64 {
	// We can calculate the write capacity units for the data size
	// DynamoDB charges 1 WCU for 1 KB of data written for On-Demand capacity mode
	// https://aws.amazon.com/dynamodb/pricing/on-demand/

	// Convert the data size from GB to KB
	// And then round up to the nearest 1 KB
	dataSizeKb := math.Ceil(dataSizeGb * math.Pow(1024, 2))
	writeCapacityUnits := dataSizeKb

	return writeCapacityUnits
}

func (im *InputManager) calculateReadCapacityUnits(dataSizeGb float64) float64 {
	// We can calculate the read capacity units for the data size
	// DynamoDB charges 1 RCU for up to 4 KB of data read for On-Demand capacity mode
	// For strongly consistent reads (What our wrapper uses)
	// https://aws.amazon.com/dynamodb/pricing/on-demand/

	// Convert the data size from GB to KB
	// And then round up to the nearest 4 KB
	dataSizeKb := dataSizeGb * 1024 * 1024
	readCapacityUnits := math.Ceil(dataSizeKb / 4)

	return readCapacityUnits
}

func (im *InputManager) GetSimulatedTransmissionInfo(
	fromInstanceIndex int,
	uninvokedInstanceIndex int,
	simulatedSyncPredecessorIndex int,
	syncNodeIndex int,
	fromRegionIndex int,
	toRegionIndex int,
	cumulativeRuntime float64,
) map[string]interface{} {
	// Convert the instance and region indices to their names
	fromInstanceName := im.InstanceIndexer.IndexToValue(fromInstanceIndex)
	uninvokedInstanceName := im.InstanceIndexer.IndexToValue(uninvokedInstanceIndex)
	simulatedSyncPredecessorName := im.InstanceIndexer.IndexToValue(simulatedSyncPredecessorIndex)
	syncNodeName := im.InstanceIndexer.IndexToValue(syncNodeIndex)
	fromRegionName := im.RegionIndexer.IndexToValue(fromRegionIndex)
	toRegionName := im.RegionIndexer.IndexToValue(toRegionIndex)

	snsTransmissionSize, transmissionLatency := im.RuntimeCalculator.CalculateSimulatedTransmissionSizeLatency(
		fromInstanceName,
		uninvokedInstanceName,
		simulatedSyncPredecessorName,
		syncNodeName,
		fromRegionName,
		toRegionName,
	)
	return map[string]interface{}{
		"starting_runtime":       cumulativeRuntime,
		"cumulative_runtime":     cumulativeRuntime + transmissionLatency,
		"sns_data_transfer_size": snsTransmissionSize,
	}
}

func (im *InputManager) GetNonExecutionInfo(
	fromInstanceIndex int,
	toInstanceIndex int,
) map[string]interface{} {
	// Convert the instance and region indices to their names
	// Start hop will never get non-execution info
	fromInstanceName := im.InstanceIndexer.IndexToValue(fromInstanceIndex)
	toInstanceName := im.InstanceIndexer.IndexToValue(toInstanceIndex)

	nonExecutionInfoList := []map[string]interface{}{}
	for syncToFromInstance, syncSize := range im.WorkflowLoader.GetNonExecutionInformation(
		fromInstanceName, toInstanceName,
	) {
		parsedSyncToFromInstance := strings.Split(syncToFromInstance, ">")
		syncPredecessorInstance := parsedSyncToFromInstance[0]
		syncNodeInstance := parsedSyncToFromInstance[1]

		nonExecutionInfoList = append(
			nonExecutionInfoList,
			map[string]interface{}{
				"predecessor_instance_id":                im.InstanceIndexer.ValueToIndex(syncPredecessorInstance),
				"sync_node_instance_id":                  im.InstanceIndexer.ValueToIndex(syncNodeInstance),
				"sync_size":                              syncSize,
				"consumed_dynamodb_write_capacity_units": 2 * im.CalculateWriteCapacity(syncSize), // We have to get sync_size * 2, as our wrapper does 2 update operations
			},
		)
	}

	return map[string]interface{}{"non_execution_info": nonExecutionInfoList}
}

func (im *InputManager) GetNodeRuntimeDataTransfer(
	instanceIndex int, regionIndex int, previousCumulativeRuntime float64, isRedirector bool,
) (map[string]interface{}, float64, float64) {
	// Convert the instance and region indices to their names
	instanceName := im.InstanceIndexer.IndexToValue(instanceIndex)
	regionName := im.RegionIndexer.IndexToValue(regionIndex)

	// Get the node runtimes and data transfer information
	return im.RuntimeCalculator.CalculateNodeRuntimeDataTransfer(
		instanceName, regionName, previousCumulativeRuntime, im.InstanceIndexer, isRedirector,
	)
}

func (im *InputManager) CalculateCostCarbonOfInstance(
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
	// Convert the instance and region indices to their names
	instanceName := im.InstanceIndexer.IndexToValue(instanceIndex)
	regionName := im.RegionIndexer.IndexToValue(regionIndex)

	dataOutputSizesStrDict := GetConvertedRegionNameMap(im.RegionIndexer, dataOutputSizes)
	executionCarbon, transmissionCarbon := im.CarbonCalculator.CalculateInstanceCarbon(
		executionTime,
		instanceName,
		regionName,
		GetConvertedRegionNameMap(im.RegionIndexer, dataInputSizes),
		dataOutputSizesStrDict,
		dataTransferDuringExecution,
		isInvoked,
		isRedirector,
	)
	return map[string]float64{
		"cost": im.CostCalculator.CalculateInstanceCost(
			executionTime,
			instanceName,
			regionName,
			dataOutputSizesStrDict,
			GetConvertedRegionNameMap(im.RegionIndexer, snsDataCallAndOutputSizes),
			dynamodbReadCapacity,
			dynamodbWriteCapacity,
			isInvoked,
		),
		"execution_carbon":    executionCarbon,
		"transmission_carbon": transmissionCarbon,
	}
}

func (im *InputManager) CalculateCostCarbonVirtualStartInstance(
	dataInputSizes map[int]float64,
	dataOutputSizes map[int]float64,
	snsDataCallAndOutputSizes map[int][]float64,
	dynamodbReadCapacity float64,
	dynamodbWriteCapacity float64,
) map[string]float64 {
	dataOutputSizesStrDict := GetConvertedRegionNameMap(im.RegionIndexer, dataOutputSizes)

	return map[string]float64{
		"cost": im.CostCalculator.CalculateVirtualStartInstanceCost(
			dataOutputSizesStrDict,
			GetConvertedRegionNameMap(im.RegionIndexer, snsDataCallAndOutputSizes),
			dynamodbReadCapacity,
			dynamodbWriteCapacity,
		),
		"execution_carbon": 0.0,
		"transmission_carbon": im.CarbonCalculator.CalculateVirtualStartInstanceCarbon(
			GetConvertedRegionNameMap(im.RegionIndexer, dataInputSizes), dataOutputSizesStrDict,
		),
	}
}

func GetConvertedRegionNameMap[V any](regionIndexer *models.RegionIndexer, inputRegionIndexDict map[int]V) map[string]V {
	systemRegionFullName := fmt.Sprintf("aws:%s", calculators.GlobalSystemRegion)
	result := map[string]V{}
	for regionIndex, value := range inputRegionIndexDict {
		if regionIndex != -1 {
			result[regionIndexer.IndexToValue(regionIndex)] = value
		} else {
			if regionIndex == -2 {
				result[systemRegionFullName] = value
			} else {
				result[""] = value
			}
		}
	}
	return result
}

func (im *InputManager) CalculateDynamoDbCapacityUnitOfSyncEdges(
	syncEdgeUploadEdgesAuxiliaryData [][]float64,
) map[string]float64 {
	// Each entry of the sync_edge_upload_edges_auxiliary_data is a tuple
	// Where the first element is when a node reaches the invoke_call
	// Where the second element is the size of the sync uploads
	// The third element is the latency of the entire tranmsission (May be used)
	// We need to first sort the list by the first element (shortest time first)
	// Then we calculate the WRU for each entry with cumulative data sizes
	writeCapacityUnits := 0.0
	cumulativeDataSize := 0.0

	// Sort the list by the first element
	sort.Slice(
		syncEdgeUploadEdgesAuxiliaryData,
		func(i, j int) bool {
			return syncEdgeUploadEdgesAuxiliaryData[i][0] < syncEdgeUploadEdgesAuxiliaryData[i][1]
		},
	)
	for _, entry := range syncEdgeUploadEdgesAuxiliaryData {
		// The entry is a tuple of (cumulative_runtime, sync_size, transmission_latency)
		syncSize := entry[1]
		cumulativeDataSize += syncSize
		writeCapacityUnits += im.CalculateWriteCapacity(cumulativeDataSize)
	}
	return map[string]float64{
		"read_capacity_units":  im.calculateReadCapacityUnits(cumulativeDataSize),
		"write_capacity_units": writeCapacityUnits,
	}
}

func (im *InputManager) GetStartHopInfo() map[string]float64 {
	workflowPlacementDecisionSizeGb := im.WorkflowLoader.GetWorkflowPlacementDecisionSize()
	readCapacityUnits := im.calculateReadCapacityUnits(workflowPlacementDecisionSizeGb)
	return map[string]float64{
		"read_capacity_units":              readCapacityUnits,
		"workflow_placement_decision_size": workflowPlacementDecisionSizeGb,
	}
}

func (im *InputManager) GetHomeRegionIndex() int {
	return im.RegionIndexer.ValueToIndex(im.WorkflowLoader.HomeRegion)
}

func (im *InputManager) TestFunc1(dataString string) float64 {
	return 1.1
}

func (im *InputManager) TestFunc2(dataString string) []float64 {
	return []float64{1.1, 1.2, 1.3}
}
