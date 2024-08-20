package calculators

import (
	"fmt"
	"log"
	"math"
	"strings"

	"caribou-go/src/deployment-input/components/loaders"
)

const GlobalSystemRegion = "us-west-2"

type CostCalculator struct {
	DatacenterLoader                  *loaders.DataCenterLoader
	WorkflowLoader                    *loaders.WorkflowLoader
	RuntimeCalculator                 *RuntimeCalculator
	ConsiderIntraRegionTransferForSNS bool
	executionConversionRatioCache     map[string][]float64
}

func NewCostCalculator(
	DatacenterLoader *loaders.DataCenterLoader,
	WorkflowLoader *loaders.WorkflowLoader,
	RuntimeCalculator *RuntimeCalculator,
) (*CostCalculator, error) {
	return &CostCalculator{
		DatacenterLoader:                  DatacenterLoader,
		WorkflowLoader:                    WorkflowLoader,
		RuntimeCalculator:                 RuntimeCalculator,
		ConsiderIntraRegionTransferForSNS: false,
		executionConversionRatioCache:     make(map[string][]float64),
	}, nil
}

func (cc *CostCalculator) CalculateVirtualStartInstanceCost(
	dataOutputSizes map[string]float64,
	snsDataCallAndOutputSizes map[string][]float64,
	dynamodbReadCapacity float64,
	dynamodbWriteCapacity float64,
) float64 {
	totalCost := 0.0

	// We model the virtual start hop cost related to dynamodb read and write
	// as accessing the system region dynamodb table.
	systemRegionName := fmt.Sprintf("aws:%s", GlobalSystemRegion)

	// Add the cost of SNS (Our current orchastration service)
	// Here we say that current region is None, such that we never
	// incur additional cost from intra-region transfer of SNS as it is not
	// ever going to be intra-region with any functions
	totalCost += cc.CalculateSNSCost("", snsDataCallAndOutputSizes)

	// We do not ever incur egress cost, as we assume client request is not
	// from a AWS or other cloud provider region and thus egrees cost is 0.0

	// Calculate the dynamodb read/write capacity cost
	totalCost += cc.CalculateDynamoDBCost(
		systemRegionName, dynamodbReadCapacity, dynamodbWriteCapacity,
	)

	return totalCost
}

func (cc *CostCalculator) CalculateInstanceCost(
	executionTime float64,
	instanceName string,
	currentRegionName string,
	dataOutputSizes map[string]float64,
	snsDataOutputSizes map[string][]float64,
	dynamodbReadCapacity float64,
	dynamodbWriteCapacity float64,
	isInvoked bool,
) float64 {
	totalCost := 0.0
	// If the function is actually invoked then
	// We must consider the cost of execution and SNS
	if isInvoked {
		// Calculate execution cost of the instance itself
		totalCost += cc.CalculateExecutionCost(instanceName, currentRegionName, executionTime)

		// Add the cost of SNS (Our current orchastration service)
		totalCost += cc.CalculateSNSCost(currentRegionName, snsDataOutputSizes)
	}

	// Even if the function is not invoked, we model
	// Each node as an abstract instance to consider
	// data transfer and dynamodb costs

	// Calculate the data transfer (egress cost) of the instance
	totalCost += cc.CalculateDataTransferCost(currentRegionName, dataOutputSizes)

	// Calculate the dynamodb read/write capacity cost
	totalCost += cc.CalculateDynamoDBCost(currentRegionName, dynamodbReadCapacity, dynamodbWriteCapacity)

	return totalCost
}

func (cc *CostCalculator) CalculateDynamoDBCost(
	currentRegionName string,
	dynamodbReadCapacity float64,
	dynamodbWriteCapacity float64,
) float64 {
	totalDynamodbCost := 0.0

	// Get the cost of dynamodb read/write capacity
	readCost, writeCost := cc.DatacenterLoader.GetDynamoDBReadWriteCost(currentRegionName)

	totalDynamodbCost += dynamodbReadCapacity * readCost
	totalDynamodbCost += dynamodbWriteCapacity * writeCost

	return totalDynamodbCost
}

func (cc *CostCalculator) CalculateSNSCost(
	currentRegionName string,
	snsDataOutputSizes map[string][]float64,
) float64 {
	totalSnsCost := 0.0

	// If assume SNS intra region data transfer is NOT free
	// We need to additionally add all SNS data transfer
	// INSIDE the current region (As data_output_size already
	// includes data transfer outside the region and contains
	// sns_data_output_sizes)
	if cc.ConsiderIntraRegionTransferForSNS && len(currentRegionName) != 0 {
		totalDataOutputSize := 0.0
		if val, exists := snsDataOutputSizes[currentRegionName]; exists {
			totalDataOutputSize = val[1]
		}

		// Calculate the cost of data transfer
		// This is simply the egress cost of data transfer
		// In a region
		// Get the cost of transmission
		transmissionCostGb := cc.DatacenterLoader.GetTransmissionCost(
			currentRegionName, true,
		)

		totalSnsCost += totalDataOutputSize * transmissionCostGb
	}

	// Get the cost of SNS invocations (Request cost of destination region)
	for regionName, snsInvocationSizes := range snsDataOutputSizes {
		// Calculate the cost of invocation
		if len(regionName) == 0 {
			log.Fatalf("Region name cannot be none")
		}
		for _, snsInvocationSizeGb := range snsInvocationSizes {
			// According to AWS documentation, each 64KB chunk of delivered data is billed as 1 request
			// https://aws.amazon.com/sns/pricing/
			// Convert gb to kb and divide by 64 rounded up
			requests := math.Ceil(snsInvocationSizeGb * math.Pow(1024, 2) / 64)
			totalSnsCost += cc.DatacenterLoader.GetSNSRequestCost(regionName) * requests
		}
	}

	return totalSnsCost
}

func (cc *CostCalculator) CalculateDataTransferCost(
	currentRegionName string,
	dataOutputSizes map[string]float64,
) float64 {
	// Calculate the amount of data output from the instance
	// This will be the data output going out of the current region
	totalDataOutputSize := 0.0
	for regionName, dataSize := range dataOutputSizes {
		if len(regionName) == 0 || !strings.HasPrefix(regionName, currentRegionName) {
			totalDataOutputSize += dataSize
		}
	}

	// Calculate the cost of data transfer
	// This is simply the egress cost of data transfer
	// In a region
	// Get the cost of transmission
	transmissionCostGb := cc.DatacenterLoader.GetTransmissionCost(
		currentRegionName, true,
	)

	return totalDataOutputSize * transmissionCostGb
}

func (cc *CostCalculator) CalculateExecutionCost(
	instanceName string,
	regionName string,
	executionTime float64,
) float64 {
	costFromComputeS, invocationCost := cc.GetExecutionConversionRatio(instanceName, regionName)
	return costFromComputeS*executionTime + invocationCost
}

func (cc *CostCalculator) GetExecutionConversionRatio(instanceName string, regionName string) (float64, float64) {
	// Check if the conversion ratio is in the cache
	key := instanceName + "_" + regionName
	if val, exists := cc.executionConversionRatioCache[key]; exists {
		return val[0], val[1]
	}

	// Get the number of vCPUs and Memory of the instance
	regionNameSplit := strings.Split(regionName, ":")
	provider := regionNameSplit[0]
	memory := cc.WorkflowLoader.GetMemory(instanceName, provider)

	// datacenter loader data
	architecture := cc.WorkflowLoader.GetArchitecture(instanceName, provider)
	computeCost := cc.DatacenterLoader.GetComputeCost(regionName, architecture)
	invocationCost := cc.DatacenterLoader.GetInvocationCost(regionName, architecture)

	// Compute cost in USD /  GB-seconds
	// Memory in MB, execution_time in seconds, vcpu in vcpu
	memoryGb := memory / 1024
	costFromComputeS := computeCost * memoryGb // IN USD / s

	// Add the conversion ratio to the cache
	cc.executionConversionRatioCache[key] = []float64{costFromComputeS, invocationCost}
	return costFromComputeS, invocationCost
}
