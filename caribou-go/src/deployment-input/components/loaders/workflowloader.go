package loaders

import (
	"fmt"
	"log"
	"math"

	"caribou-go/src/utils"
)

const (
	SolverInputRuntimeDefault = -1.0 // Denotes that the runtime is not available
	SolverInputLatencyDefault = -1.0 // Denotes that the latency is not available
)

const (
	SolverInputDataTransferSizeDefault            = 0.0
	SolverInputInvocationProbabilityDefault       = 0.0 // If it is missing, the invocation is never called in the workflow
	SolverInputProjectedMonthlyInvocationsDefault = 0.0
)

const (
	SolverInputVcpuDefault         = -1.0 // Denotes that the vCPU is not available
	SolverInputArchitectureDefault = "x86_64"
)

const (
	SyncSizeDefault = 1.0 / 1048576 // 1 KB in GB
	SnsSizeDefault  = 1.0 / 1048576 // 1 KB in GB
)

type WorkflowLoader struct {
	WorkflowData                     map[string]interface{}
	InstanceRegionsProviders         map[string]interface{}
	HomeRegion                       string
	PerformanceLoader                *PerformanceLoader
	dataTransferSizeCache            map[string][]float64
	startHopSizeCache                map[string][]float64
	runtimeDistributionCache         map[string][]float64
	startHopLatencyDistributionCache map[string][]float64
}

func NewWorkflowLoader(
	workflowData map[string]interface{}, instanceRegionsProviders map[string]interface{},
	homeRegion string, performanceLoader *PerformanceLoader,
) (*WorkflowLoader, error) {
	workflowLoader := WorkflowLoader{
		WorkflowData:                     workflowData,
		InstanceRegionsProviders:         instanceRegionsProviders,
		HomeRegion:                       homeRegion,
		PerformanceLoader:                performanceLoader,
		dataTransferSizeCache:            make(map[string][]float64),
		startHopLatencyDistributionCache: make(map[string][]float64),
		runtimeDistributionCache:         make(map[string][]float64),
		startHopSizeCache:                make(map[string][]float64),
	}
	return &workflowLoader, nil
}

func (w *WorkflowLoader) GetWorkflowPlacementDecisionSize() float64 {
	// Workflow Placement Decision Size
	return utils.Get(w.WorkflowData, 0.0, "start_hop_summary", "workflow_placement_decision_size_gb")
}

func (w *WorkflowLoader) GetStartHopRetrieveWpdProbability() float64 {
	return utils.Get(w.WorkflowData, 1.0, "start_hop_summary", "wpd_at_function_probability")
}

func (w *WorkflowLoader) GetStartHopSizeDistribution() []float64 {
	// Start hop size distribution, if not available, return the WPD size
	// As it will always send at least the WPD size
	return utils.GetList(
		w.WorkflowData,
		[]float64{w.GetWorkflowPlacementDecisionSize()},
		"start_hop_summary",
		"from_client",
		"transfer_sizes_gb",
	)
}

func (w *WorkflowLoader) GetStartHopBestFitLine(toRegionName string) *map[string]float64 {
	// Start hop size distribution, if not available, return the WPD size
	// As it will always send at least the WPD size
	bestFit := map[string]float64{}
	result := utils.Get(
		w.WorkflowData, map[string]interface{}{},
		"start_hop_summary", "from_client", "received_region", "regions_to_regions", toRegionName, "best_fit_line",
	)
	for k, v := range result {
		bestFit[k] = v.(float64)
	}
	return &bestFit
}

func (w *WorkflowLoader) GetStartHopLatencyDistribution(toRegionName string, dataTransferSize float64) []float64 {
	cacheKey := fmt.Sprintf("%s_%v", toRegionName, dataTransferSize)
	if distribution, exists := w.startHopLatencyDistributionCache[cacheKey]; exists {
		return distribution
	}
	// Round data transfer size translation to nearest 10 KB
	dataTransferSizeKb := w.RoundToKb(dataTransferSize, 10, true)
	startHopLatencyDistribution := utils.GetList(
		w.WorkflowData, []float64{},
		"start_hop_summary",
		"from_client",
		"received_region",
		toRegionName,
		"transfer_size_gb_to_transfer_latencies_s",
		fmt.Sprintf("%v", dataTransferSize),
	)
	if len(startHopLatencyDistribution) == 0 {
		// Atempt to use the best fit line size
		bestFitLine := w.GetStartHopBestFitLine(toRegionName)
		if bestFitLine != nil && len(*bestFitLine) != 0 {
			// Estimate the latency using the best fit line
			estimatedLatency := (*bestFitLine)["slope_s"]*dataTransferSizeKb + (*bestFitLine)["intercept_s"]

			// Limit the estimated latency to the min and max latency
			estimatedLatency = min((*bestFitLine)["max_latency_s"], max((*bestFitLine)["min_latency_s"], estimatedLatency))

			startHopLatencyDistribution = []float64{estimatedLatency}
		}
	}
	w.startHopLatencyDistributionCache[cacheKey] = startHopLatencyDistribution
	return startHopLatencyDistribution
}

func (w *WorkflowLoader) GetAverageCpuUtilization(instanceName string, regionName string, isRedirector bool) float64 {
	var executionsData map[string]interface{}
	if !isRedirector {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "instance_summary")
	} else {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "start_hop_summary", "at_redirector")
	}

	// Get the average CPU utilization for the instance (at specific region if possible
	// if not then try to get the average cpu utilization for the instance on all regions)
	// If that is not even available (Should be impossible), default to 0.5 (Average cpu utilization of
	// hyperscale cloud providers)
	cpuUtil := utils.Get(executionsData, -1.0, instanceName, "executions", "at_region", regionName, "cpu_utilization")
	if cpuUtil < 0 {
		cpuUtil = utils.Get(executionsData, 0.5, instanceName, "cpu_utilization")
	}
	return cpuUtil
}

func (w *WorkflowLoader) GetRuntimeDistribution(instanceName string, regionName string, isRedirector bool) []float64 {
	var executionsData map[string]interface{}
	if !isRedirector {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "instance_summary")
	} else {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "start_hop_summary", "at_redirector")
	}

	return utils.GetList(
		executionsData,
		[]float64{},
		instanceName,
		"executions",
		"at_region",
		regionName,
		"durations_s",
	)
}

func (w *WorkflowLoader) GetAuxiliaryDataDistribution(instanceName string, regionName string, runtime float64, isRedirector bool) [][]float64 {
	// Round the duration to the nearest 10 ms
	runtime = w.RoundToMs(runtime, 10, true)
	runtimeStr := fmt.Sprintf("%.2f", runtime)
	if runtimeStr[len(runtimeStr)-1] == '0' {
		runtimeStr = runtimeStr[:len(runtimeStr)-1]
	}

	var executionsData map[string]interface{}
	if !isRedirector {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "instance_summary")
	} else {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "start_hop_summary", "at_redirector")
	}

	return utils.GetList(
		executionsData,
		[][]float64{},
		instanceName,
		"executions",
		"at_region",
		regionName,
		"auxiliary_data",
		runtimeStr,
	)
}

func (w *WorkflowLoader) GetAuxiliaryIndexTranslation(instanceName string, isRedirector bool) map[string]int {
	var executionsData map[string]interface{}
	if !isRedirector {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "instance_summary")
	} else {
		executionsData = utils.Get(w.WorkflowData, map[string]interface{}{}, "start_hop_summary", "at_redirector")
	}

	return utils.Get(
		executionsData,
		map[string]int{},
		instanceName,
		"executions",
		"auxiliary_index_translation",
	)
}

func (w *WorkflowLoader) GetInvocationProbability(fromInstanceName string, toInstanceName string) float64 {
	if fromInstanceName == toInstanceName {
		return 1
	}
	return utils.Get(w.WorkflowData, SolverInputInvocationProbabilityDefault, "instance_summary", fromInstanceName, "to_instance", toInstanceName, "invocation_probability")
}

func (w *WorkflowLoader) GetDataTransferSizeDistribution(
	fromInstanceName string,
	toInstanceName string,
) []float64 {
	cacheKey := fmt.Sprintf("%s_%s", fromInstanceName, toInstanceName)
	if distribution, exists := w.dataTransferSizeCache[cacheKey]; exists {
		return distribution
	}
	resultSize := utils.GetList(w.WorkflowData, []float64{}, "instance_summary", fromInstanceName, "to_instance", toInstanceName, "transfer_sizes_gb")
	w.dataTransferSizeCache[cacheKey] = resultSize
	return resultSize
}

func (w *WorkflowLoader) GetLatencyDistributionBestFitLine(
	fromInstanceName string,
	toInstanceName string,
	fromRegionName string,
	toRegionName string,
) *map[string]float64 {
	var bestFitLine map[string]float64
	return utils.Get(
		w.WorkflowData,
		&bestFitLine,
		"instance_summary",
		fromInstanceName,
		"to_instance",
		toInstanceName,
		"regions_to_regions",
		fromRegionName,
		toRegionName,
		"best_fit_line",
	)
}

func (w *WorkflowLoader) GetLatencyDistribution(
	fromInstanceName string,
	toInstanceName string,
	fromRegionName string,
	toRegionName string,
	dataTransferSize float64,
) []float64 {
	dataTransferSize = w.RoundToKb(dataTransferSize, 10, true)
	latencyDistribution := utils.GetList(
		w.WorkflowData, []float64{},
		"instance_summary", fromInstanceName, "to_instance",
		toInstanceName, "regions_to_regions", fromRegionName, toRegionName,
		"transfer_size_gb_to_transfer_latencies_s", fmt.Sprintf("%v", dataTransferSize),
	)
	if len(latencyDistribution) == 0 {
		// Attempt to use the best fit line size
		bestFitLine := w.GetLatencyDistributionBestFitLine(fromInstanceName, toInstanceName, fromRegionName, toInstanceName)
		if bestFitLine != nil && len(*bestFitLine) != 0 {
			// Estimate the latency using the best fit line
			estimatedLatency := (*bestFitLine)["slope_s"]*dataTransferSize + (*bestFitLine)["intercept_s"]

			// Limit the estimated latency to the min and max latency
			estimatedLatency = min((*bestFitLine)["max_latency_s"], max((*bestFitLine)["min_latency_s"], estimatedLatency))

			latencyDistribution = []float64{estimatedLatency}
		}
	}
	return latencyDistribution
}

func (w *WorkflowLoader) GetNonExecutionInformation(
	fromInstanceName string, toInstanceName string,
) map[string]float64 {
	nonExecutionInfoDict := map[string]float64{}
	nonExecMap := utils.Get(
		w.WorkflowData,
		map[string]interface{}{}, "instance_summary", fromInstanceName, "to_instance", toInstanceName, "non_execution_info",
	)
	for key, value := range nonExecMap {
		valueMap := value.(map[string]interface{})
		nonExecutionInfoDict[key] = utils.Get(valueMap, 0.0, "sync_data_response_size_gb")
	}
	return nonExecutionInfoDict
}

func (w *WorkflowLoader) GetNonExecutionSNSTransferSize(fromInstanceName string, toInstanceName string, syncToFromInstance string) float64 {
	// Round to the nearest non-zero KB
	// (At least 1 byte of data is transferred for sns)
	return w.RoundToKb(
		utils.Get(w.WorkflowData, 0.0, "instance_summary", fromInstanceName, "to_instance", toInstanceName, "non_execution_info", syncToFromInstance, "sns_transfer_size_gb"),
		1,
		false,
	)
}

func (w *WorkflowLoader) GetNonExecutionTransferLatencyDistribution(
	fromInstanceName string,
	toInstanceName string,
	syncToFromInstance string,
	fromRegionName string,
	toRegionName string,
) []float64 {
	return utils.GetList(
		w.WorkflowData,
		[]float64{},
		"instance_summary",
		fromInstanceName,
		"to_instance",
		toInstanceName,
		"non_execution_info",
		syncToFromInstance,
		"regions_to_regions",
		fromRegionName,
		toRegionName,
		"transfer_latencies_s",
	)
}

func (w *WorkflowLoader) GetSyncSize(fromInstanceName string, toInstanceName string) float64 {
	return utils.Get(
		w.WorkflowData,
		SyncSizeDefault,
		"instance_summary",
		fromInstanceName,
		"to_instance",
		toInstanceName,
		"sync_sizes_gb",
	)
}

func (w *WorkflowLoader) GetSNSOnlySize(fromInstanceName string, toInstanceName string) float64 {
	return utils.Get(
		w.WorkflowData,
		SnsSizeDefault,
		"instance_summary",
		fromInstanceName,
		"to_instance",
		toInstanceName,
		"sns_only_sizes_gb",
	)
}

func (w *WorkflowLoader) GetVCpu(instanceName string, providerName string) float64 {
	vcpu := utils.Get(w.InstanceRegionsProviders, SolverInputVcpuDefault, instanceName, providerName, "config", "vcpu")
	if vcpu < 0 {
		if providerName == "aws" {
			return w.GetMemory(instanceName, providerName) / 1769
		} else {
			log.Fatalf("vCPU count for instance %s in provider %s is not available", instanceName, providerName)
		}
	}
	return vcpu
}

func (w *WorkflowLoader) GetMemory(instanceName string, providerName string) float64 {
	mem := utils.Get(w.InstanceRegionsProviders, -1.0, instanceName, providerName, "config", "memory")
	if mem == -1.0 {
		log.Fatalf("Memory not found: %s - %s", instanceName, providerName)
	}
	return mem
}

func (w *WorkflowLoader) GetArchitecture(instanceName string, providerName string) string {
	return utils.Get(w.InstanceRegionsProviders, SolverInputArchitectureDefault, instanceName, providerName, "config", "architecture")
}

func (w *WorkflowLoader) RoundToKb(val float64, roundTo int, roundUp bool) float64 {
	/**
	Rounds the input number (in GB) to the nearest KB or 10 KB in base 2, rounding up
	or to the nearest non_zero.

		:param number: The input number in GB.
		:param round_to: The value to round to (1 for nearest KB, 10 for nearest 10 KB).
		:param round_up: Whether to round up or to nearest non-zero KB.
		:return: The rounded number in GB.
	**/
	roundedKb := val * math.Pow(1024, 2) / float64(roundTo)
	if roundUp {
		roundedKb = math.Ceil(roundedKb)
	} else {
		// Round to the nearest non-zero
		roundedKb = math.Floor(roundedKb + 0.5)
		if roundedKb == 0.0 {
			roundedKb = 1.0
		}
	}

	return roundedKb * float64(roundTo) / math.Pow(1024, 2)
}

func (w *WorkflowLoader) RoundToMs(number float64, roundTo int, roundUp bool) float64 {
	/**
	Rounds the input number (in seconds) to the nearest ms, rounding up
	or to the nearest non_zero.

	:param number: The input number in seconds.
	:param round_to: The value to round to (1 for nearest ms, 10 for nearest 10 ms).
	:param round_up: Whether to round up or to nearest non-zero ms.
	:return: The rounded number in seconds.
	*/
	roundedMs := number * 1000.0 / float64(roundTo)
	if roundUp {
		roundedMs = math.Ceil(roundedMs)
	} else {
		// Round to the nearest non-zero
		roundedMs = math.Floor(roundedMs + 0.5)
		if roundedMs == 0.0 {
			roundedMs = 1.0
		}
	}
	return roundedMs * float64(roundTo) / 1000.0
}
