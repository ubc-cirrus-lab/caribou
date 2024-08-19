package calculators

import (
	"fmt"
	"math/rand"

	"caribou-go/src/deployment-input/components/loaders"
	"caribou-go/src/models"

	"gonum.org/v1/gonum/stat"
)

const SolverHomeRegionTransmissionLatencyDefault = 0.22 // TODO: CHANGE THIS, this should be what the average transmission latency at home region between 2 SNS calls

type RuntimeCalculator struct {
	PerformanceLoader                    *loaders.PerformanceLoader
	WorkflowLoader                       *loaders.WorkflowLoader
	transmissionLatencyDistributionCache map[string][]float64
	transmissionSizeDistributionCache    map[string][]float64
}

func NewRuntimeCalculator(
	performanceLoader *loaders.PerformanceLoader,
	workflowLoader *loaders.WorkflowLoader,
) (*RuntimeCalculator, error) {
	calculator := RuntimeCalculator{
		PerformanceLoader: performanceLoader, WorkflowLoader: workflowLoader,
		transmissionLatencyDistributionCache: make(map[string][]float64),
		transmissionSizeDistributionCache:    make(map[string][]float64),
	}
	return &calculator, nil
}

func (rc *RuntimeCalculator) ResetCache() {
	rc.transmissionLatencyDistributionCache = make(map[string][]float64)
	rc.transmissionSizeDistributionCache = make(map[string][]float64)
}

func (rc *RuntimeCalculator) CalculateSimulatedTransmissionSizeLatency(
	fromInstanceName string,
	uninvokedInstanceName string,
	simulatedSyncPredecessorName string,
	syncNodeName string,
	fromRegionName string,
	toRegionName string,
) (float64, float64) {
	syncToFromInstance := fmt.Sprintf("%s>%s", simulatedSyncPredecessorName, syncNodeName)

	// Get the average transmission size of the from_instance to the sync node from
	// the non-execution information
	transmissionSize := rc.WorkflowLoader.GetNonExecutionSNSTransferSize(fromInstanceName, uninvokedInstanceName, syncToFromInstance)

	// Get the non-execution transmission latency distribution of the input size (If it exists)
	transmissionLatencyDistribution := rc.WorkflowLoader.GetNonExecutionTransferLatencyDistribution(
		fromInstanceName,
		uninvokedInstanceName,
		syncToFromInstance,
		fromRegionName,
		toRegionName,
	)

	if len(transmissionLatencyDistribution) == 0 {
		// Default to transmission latency distribution of what happens when
		// simulated_sync_predecessor_name Calls sync_node_name as a normal transmission
		// While this might not be the most accurate, it is the best we can do in this case
		// However, this can be improved in the future.
		// This type of transmission will always go to a sync node
		transmissionLatencyDistribution = rc.GetTransmissionLatencyDistribution(
			simulatedSyncPredecessorName, fromRegionName, syncNodeName, toRegionName, transmissionSize, true, false, // Does not affect the latency (as it is never the start hop)
		)
	}

	// Pick a transmission latency
	transmissionLatency := transmissionLatencyDistribution[rand.Int()%len(transmissionLatencyDistribution)]

	return transmissionSize, transmissionLatency
}

func (rc *RuntimeCalculator) CalculateTransmissionSizeLatency(
	fromInstanceName string,
	fromRegionName string,
	toInstanceName string,
	toRegionName string,
	isSyncPredecessor bool,
	considerFromClientLatency bool,
) (float64, float64) {
	// Here we pick a random data transfer size, then pick a random latency

	// Get the transmission size distribution
	transmissionSizeDistribution := rc.GetTransmissionSizeDistribution(
		fromInstanceName, toInstanceName,
	)

	// Pick a transmission size
	transmissionSize := transmissionSizeDistribution[rand.Int()%len(transmissionSizeDistribution)]

	// Get the transmission latency distribution of the input size
	transmissionLatencyDistribution := rc.GetTransmissionLatencyDistribution(
		fromInstanceName, fromRegionName, toInstanceName, toRegionName, transmissionSize, isSyncPredecessor, considerFromClientLatency,
	)

	transmissionLatency := transmissionLatencyDistribution[rand.Int()%len(transmissionLatencyDistribution)]

	return transmissionSize, transmissionLatency
}

func (rc *RuntimeCalculator) GetTransmissionLatencyDistribution(
	fromInstanceName string,
	fromRegionName string,
	toInstanceName string,
	toRegionName string,
	dataTransferSize float64,
	isSyncPredecessor bool,
	considerFromClientLatency bool,
) []float64 {
	cacheKey := fmt.Sprintf("%s-%s-%s-%s-%v", fromInstanceName, toInstanceName, fromRegionName, toRegionName, dataTransferSize)
	if distribution, exists := rc.transmissionLatencyDistributionCache[cacheKey]; exists {
		return distribution
	}
	var tlDistribution []float64
	if len(fromInstanceName) > 0 && len(fromRegionName) > 0 {
		tlDistribution = rc.WorkflowLoader.GetLatencyDistribution(
			fromInstanceName, toInstanceName, fromRegionName, toRegionName, dataTransferSize,
		)
		if len(tlDistribution) == 0 {
			tlDistribution = rc.HandleMissingTransmissionLatencyDistribution(
				fromInstanceName, fromRegionName, toInstanceName, toRegionName, dataTransferSize, isSyncPredecessor)
		}
	} else {
		tlDistribution = []float64{0.0}
		if considerFromClientLatency {
			tlDistribution = rc.WorkflowLoader.GetStartHopLatencyDistribution(
				toRegionName, dataTransferSize,
			)
			if len(tlDistribution) == 0 {
				tlDistribution = rc.HandleMissingStartHopLatencyDistribution(toRegionName, dataTransferSize)
			}
		}
	}
	if len(tlDistribution) == 0 {
		panic(fmt.Errorf(
			"the transmission latency distribution for %s to %s for %s to %s is empty, this should be impossible",
			fromInstanceName, toInstanceName, fromRegionName, toRegionName,
		))
	}
	rc.transmissionLatencyDistributionCache[cacheKey] = tlDistribution
	return tlDistribution
}

func (rc *RuntimeCalculator) HandleMissingTransmissionLatencyDistribution(
	fromInstanceName string,
	fromRegionName string,
	toInstanceName string,
	toRegionName string,
	dataTransferSize float64,
	isSyncPredecessor bool,
) []float64 {
	// No size information, we rely on performance loader to get the transmission latency
	// between two regions from cloud ping.
	cloudPingTransmissionLatencyDistribution := rc.PerformanceLoader.GetTransmissionLatencyDistribution(
		fromRegionName, toRegionName,
	)
	averageCloudPingTransmissionLatencyPerformance := stat.Mean(
		cloudPingTransmissionLatencyDistribution, nil,
	)

	homeRegionName := rc.WorkflowLoader.HomeRegion
	cloudPingHomeRegionLatencyDistributionPerformance := rc.PerformanceLoader.GetTransmissionLatencyDistribution(
		homeRegionName, homeRegionName,
	)
	averageCloudPingHomeRegionLatencyPerformance := stat.Mean(cloudPingHomeRegionLatencyDistributionPerformance, nil)

	// Calculate the difference in latency between the home region and the current region (Should never be below 0)
	averageCloudPingLatencyDifference := max(
		averageCloudPingTransmissionLatencyPerformance-averageCloudPingHomeRegionLatencyPerformance,
		0.0,
	)

	// Get the measure latency from the home region (actual latency)
	homeRegionLatencyDistributionMeasured := rc.WorkflowLoader.GetLatencyDistribution(
		fromInstanceName, toInstanceName, homeRegionName, homeRegionName, dataTransferSize,
	)
	if len(homeRegionLatencyDistributionMeasured) == 0 {
		// For cases where its a sync predecessor, there might be no latency data
		// even for the home region, in this case we default to the average latency between
		// two of the same region (A default value)
		homeRegionLatencyDistributionMeasured = []float64{SolverHomeRegionTransmissionLatencyDefault}
	}
	// Calculate the multiplier to apply to the added latency
	// For sync nodes this would be x(1 + 4), as it involves one update to sync_decision_table
	// and one upload to s3, which is 4 times the latency of a normal transmission
	multiplier := 1.0
	if isSyncPredecessor {
		multiplier += 4.0
	}

	// Estimate the actual transmission latency distribution by adding the difference in latency
	// between the home region and the current region to the cloud ping latency
	addedLatencyDistribution := make([]float64, len(homeRegionLatencyDistributionMeasured))
	for i, latency := range homeRegionLatencyDistributionMeasured {
		addedLatencyDistribution[i] = latency + averageCloudPingLatencyDifference*multiplier
	}

	return addedLatencyDistribution
}

func (rc *RuntimeCalculator) HandleMissingStartHopLatencyDistribution(toRegionName string, dataTransferSize float64) []float64 {
	homeRegion := rc.WorkflowLoader.HomeRegion
	if homeRegion == toRegionName {
		panic(fmt.Errorf("start hop latency distribution for home region is empty, this should be impossible"))
	}
	// At this point we are in a region that the instance was never invoked
	// Thus we have no data transfer size distribution
	// In this case we try to estimate the latency using the transmission latency
	// Based on the transmission latency distribution, here we assume that we can estimate the latency
	// By adding estimated transmission latency (cloud ping) to the start hop latency
	homeDistribution := rc.WorkflowLoader.GetStartHopLatencyDistribution(homeRegion, dataTransferSize)
	tlDistribution := rc.PerformanceLoader.GetTransmissionLatencyDistribution(
		homeRegion, toRegionName,
	)
	startHopDistribution := make([]float64, len(homeDistribution))
	for i, homeDist := range homeDistribution {
		startHopDistribution[i] = homeDist + tlDistribution[i%len(tlDistribution)]
	}
	return startHopDistribution
}

func (rc *RuntimeCalculator) GetTransmissionSizeDistribution(
	fromInstanceName string,
	toInstanceName string,
) []float64 {
	cacheKey := fromInstanceName + "-" + toInstanceName
	if distribution, exists := rc.transmissionSizeDistributionCache[cacheKey]; exists {
		return distribution
	}
	var tsDistribution []float64
	if len(fromInstanceName) > 0 {
		tsDistribution = rc.WorkflowLoader.GetDataTransferSizeDistribution(
			fromInstanceName, toInstanceName,
		)
	} else {
		tsDistribution = rc.WorkflowLoader.GetStartHopSizeDistribution()
	}
	if len(tsDistribution) == 0 {
		// There should never be a case where the size distribution is empty
		// As it would just mean that the instance was never invoked
		// But then it should never had reached this point (as it would have 0% inv probability)
		panic(
			fmt.Errorf(
				"size distribution for %s to %s is empty, this should be impossible",
				fromInstanceName, toInstanceName))
	}
	rc.transmissionSizeDistributionCache[cacheKey] = tsDistribution
	return tsDistribution
}

func (rc *RuntimeCalculator) CalculateNodeRuntimeDataTransfer(
	instanceName string,
	regionName string,
	previousCumulativeRuntime float64,
	instanceIndexer *models.InstanceIndexer,
	isRedirector bool,
) (map[string]interface{}, float64, float64) {
	// Calculate the current runtime of this instance when executed in the given region
	// Get the runtime distribution of the instance in the given region
	runtimeDistribution := rc.WorkflowLoader.GetRuntimeDistribution(instanceName, regionName, isRedirector)
	originalRuntimeRegionName := regionName
	desiredRuntimeRegionName := regionName
	if len(runtimeDistribution) == 0 {
		// No runtime data for this instance in this region, default to home region
		homeRegion := rc.WorkflowLoader.HomeRegion
		if homeRegion == regionName {
			// This should never happen, as the instance should have been invoked in the home region
			// At least once, thus it should have runtime data
			panic(fmt.Errorf(
				"instance %s has no runtime data in home region %s, this should be impossible", instanceName, regionName),
			)
		}
		runtimeDistribution = rc.WorkflowLoader.GetRuntimeDistribution(instanceName, homeRegion, isRedirector)
		originalRuntimeRegionName = homeRegion
	}
	runtime := runtimeDistribution[rand.Int()%len(runtimeDistribution)]
	return rc.RetrieveRuntimeDataTransfer(instanceName, originalRuntimeRegionName, desiredRuntimeRegionName, runtime, previousCumulativeRuntime, instanceIndexer, isRedirector)
}

func (rc *RuntimeCalculator) RetrieveRuntimeDataTransfer(
	instanceName string,
	originalRuntimeRegionName string,
	desiredRuntimeRegionName string,
	runtime float64,
	previousCumulativeRuntime float64,
	instanceIndexer *models.InstanceIndexer,
	isRedirector bool,
) (map[string]interface{}, float64, float64) {
	// Retrieve the auxiliary_index_translation
	auxiliaryIndexTranslation := rc.WorkflowLoader.GetAuxiliaryIndexTranslation(instanceName, isRedirector)

	// Get the auxiliary data distribution of the instance in the given region
	// This can be cached in the future for performance improvements
	executionAuxiliaryData := rc.WorkflowLoader.GetAuxiliaryDataDistribution(instanceName, originalRuntimeRegionName, runtime, isRedirector)

	// Pick a random auxiliary data from the distribution
	auxiliaryData := executionAuxiliaryData[rand.Int()%len(executionAuxiliaryData)]

	// Calculate the relative region performance
	// to original region
	relativeRegionPerformance := 1.0
	if originalRuntimeRegionName != desiredRuntimeRegionName {
		// Get the relative performance of the region
		originalRegionPerformance := rc.PerformanceLoader.GetRelativePerformance(originalRuntimeRegionName)
		desiredRegionPerformance := rc.PerformanceLoader.GetRelativePerformance(desiredRuntimeRegionName)
		relativeRegionPerformance = desiredRegionPerformance / originalRegionPerformance
	}

	// Create the successor dictionary
	// Go through the auxiliary translation index and get every value other than data_transfer_during_execution_gb
	successorsRuntimeData := map[int]float64{}
	for key, index := range auxiliaryIndexTranslation {
		if key != "data_transfer_during_execution_gb" {
			successorsRuntimeData[instanceIndexer.ValueToIndex(key)] = previousCumulativeRuntime + auxiliaryData[index]*relativeRegionPerformance
		}
	}

	// The key is the instance index of the successor
	// This need to be translated from index to instance name in the
	// input manager
	// The value is the cumulative runtime of when this
	// node invokes the successor
	currentNodeExecutionTime := runtime * relativeRegionPerformance
	return map[string]interface{}{
		"current":    previousCumulativeRuntime + currentNodeExecutionTime,
		"successors": successorsRuntimeData,
	}, currentNodeExecutionTime, auxiliaryData[auxiliaryIndexTranslation["data_transfer_during_execution_gb"]]
}
