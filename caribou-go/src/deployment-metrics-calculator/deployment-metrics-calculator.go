package deployment_metrics_calculator

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"strconv"

	deploymentinput "caribou-go/src/deployment-input"
	"caribou-go/src/deployment-metrics-calculator/models"

	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"
)

type DeploymentMetricsCalculator struct {
	inputManager                      *deploymentinput.InputManager
	tailLatencyThreshold              float64
	prerequisitesDictionary           map[int][]int
	successorDictionary               map[int][]int
	topologicalOrder                  []int
	homeRegionIndex                   int
	recordTransmissionExecutionCarbon bool
	considerFromClientLatency         bool
}

func (dc *DeploymentMetricsCalculator) CalculateWorkflow(deployment []int) map[string]float64 {
	startHopIndex := dc.topologicalOrder[0]
	workflowInstance := models.NewWorkflowInstance(dc.inputManager, deployment, startHopIndex, dc.considerFromClientLatency)
	for _, instanceIndex := range dc.topologicalOrder {
		predecessorInstanceIndices := dc.prerequisitesDictionary[instanceIndex]
		if len(predecessorInstanceIndices) == 0 {
			workflowInstance.AddStartHop(instanceIndex)
		}
		nodeInvoked := workflowInstance.AddNode(instanceIndex)
		for _, successorIndex := range dc.successorDictionary[instanceIndex] {
			isInvoked := false
			if nodeInvoked {
				isInvoked = dc.isInvoked(instanceIndex, successorIndex)
			}
			workflowInstance.AddEdge(instanceIndex, successorIndex, isInvoked)
		}
	}
	workflowMetrics := workflowInstance.CalculateOverallCostRuntimeCarbon()
	return workflowMetrics
}

func (dc *DeploymentMetricsCalculator) isInvoked(fromInstance int, toInstance int) bool {
	invocationProbability := dc.inputManager.GetInvocationProbability(fromInstance, toInstance)
	return rand.Float64() < invocationProbability
}

func SetupDeploymentMetricsCalculator(dataString string) *DeploymentMetricsCalculator {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(dataString), &data)
	if err != nil {
		fmt.Println("Error unmarshaling JSON:", err)
		return nil
	}
	inputManager := deploymentinput.Setup(
		data["input_manager"].(map[string]interface{}),
	)
	prerequisitesDictionary := map[int][]int{}
	for k, v := range data["prerequisites_dictionary"].(map[string]interface{}) {
		vSlice := v.([]interface{})
		kInt, _ := strconv.Atoi(k)
		prerequisitesDictionary[kInt] = make([]int, len(vSlice))
		for i, elemV := range vSlice {
			prerequisitesDictionary[kInt][i] = int(elemV.(float64))
		}
	}
	successorDictionary := map[int][]int{}
	for k, v := range data["successor_dictionary"].(map[string]interface{}) {
		vSlice := v.([]interface{})
		kInt, _ := strconv.Atoi(k)
		successorDictionary[kInt] = make([]int, len(vSlice))
		for i, elemV := range vSlice {
			successorDictionary[kInt][i] = int(elemV.(float64))
		}
	}
	topologicalOrder := []int{}
	for _, v := range data["topological_order"].([]interface{}) {
		topologicalOrder = append(topologicalOrder, int(v.(float64)))
	}
	return &DeploymentMetricsCalculator{
		inputManager:                      inputManager,
		tailLatencyThreshold:              data["tail_latency_threshold"].(float64),
		prerequisitesDictionary:           prerequisitesDictionary,
		successorDictionary:               successorDictionary,
		topologicalOrder:                  topologicalOrder,
		homeRegionIndex:                   int(data["home_region_index"].(float64)),
		recordTransmissionExecutionCarbon: data["record_transmission_execution_carbon"].(bool),
	}
}

type SimpleDeploymentMetricsCalculator struct {
	DeploymentMetricsCalculator
	batchSize int
}

func SetupSimpleDeploymentMetricsCalculator(dataString string) *SimpleDeploymentMetricsCalculator {
	return &SimpleDeploymentMetricsCalculator{
		*SetupDeploymentMetricsCalculator(dataString),
		200,
	}
}

func (sd *SimpleDeploymentMetricsCalculator) CalculateWorkflowLoop(deployment []int) (
	[]float64, []float64, []float64, []float64, []float64,
) {
	costsDistributionList := []float64{}
	runtimesDistributionList := []float64{}
	carbonsDistributionList := []float64{}
	transmissionCarbonList := []float64{}
	executionCarbonList := []float64{}
	for range sd.batchSize {
		results := sd.CalculateWorkflow(deployment)
		costsDistributionList = append(costsDistributionList, results["cost"])
		runtimesDistributionList = append(runtimesDistributionList, results["runtime"])
		carbonsDistributionList = append(carbonsDistributionList, results["carbon"])

		if sd.recordTransmissionExecutionCarbon {
			transmissionCarbonList = append(transmissionCarbonList, results["transmission_carbon"])
			executionCarbonList = append(executionCarbonList, results["execution_carbon"])
		}
	}
	return costsDistributionList,
		runtimesDistributionList,
		carbonsDistributionList,
		transmissionCarbonList,
		executionCarbonList
}

func (sd *SimpleDeploymentMetricsCalculator) PerformMonteCarloSimulation(deployment []int) map[string]float64 {
	/**
	Perform a Monte Carlo simulation to both the average and tail
	cost, runtime, and carbon footprint of the deployment.
	*/
	costsDistributionList := []float64{}
	runtimesDistributionList := []float64{}
	carbonsDistributionList := []float64{}

	executionCarbonList := []float64{}
	transmissionCarbonList := []float64{}

	maxNumberOfIterations := 2000
	threshold := 0.05
	numberOfIterations := 0

	for numberOfIterations < maxNumberOfIterations {
		costsDistribution,
			runtimesDistribution,
			carbonsDistribution,
			transmissionCarbon,
			executionCarbon := sd.CalculateWorkflowLoop(deployment)
		costsDistributionList = append(costsDistributionList, costsDistribution...)
		runtimesDistributionList = append(runtimesDistributionList, runtimesDistribution...)
		carbonsDistributionList = append(carbonsDistributionList, carbonsDistribution...)
		if sd.recordTransmissionExecutionCarbon {
			transmissionCarbonList = append(transmissionCarbonList, transmissionCarbon...)
			executionCarbonList = append(executionCarbonList, executionCarbon...)
		}
		numberOfIterations += sd.batchSize
		allWithinThreshold := true
		for _, distribution := range [][]float64{
			costsDistribution,
			runtimesDistribution,
			carbonsDistribution,
			transmissionCarbon,
			executionCarbon,
		} {
			mean := stat.Mean(distribution, nil)
			sem := stat.StdErr(stat.StdDev(distribution, nil), float64(len(distribution)))

			// Calculate the t-distribution critical value
			alpha := 1 - threshold
			criticalValue := distuv.StudentsT{
				Mu:    0,
				Sigma: 1,
				Nu:    float64(len(distribution) - 1),
			}.Quantile(1 - alpha/2)

			// Calculate the confidence interval
			ciLow := mean - criticalValue*sem
			ciUp := mean + criticalValue*sem
			relativeCiWidth := (ciUp - ciLow) / mean
			if relativeCiWidth > threshold {
				allWithinThreshold = false
				break
			} else if allWithinThreshold {
				break
			}
		}

	}
	slices.Sort(costsDistributionList)
	slices.Sort(runtimesDistributionList)
	slices.Sort(carbonsDistributionList)
	result := map[string]float64{
		"average_cost":    stat.Mean(costsDistributionList, nil),
		"average_runtime": stat.Mean(runtimesDistributionList, nil),
		"average_carbon":  stat.Mean(carbonsDistributionList, nil),
		"tail_cost":       stat.Quantile(sd.tailLatencyThreshold/100, stat.Empirical, costsDistributionList, nil),
		"tail_runtime":    stat.Quantile(sd.tailLatencyThreshold/100, stat.Empirical, runtimesDistributionList, nil),
		"tail_carbon":     stat.Quantile(sd.tailLatencyThreshold/100, stat.Empirical, carbonsDistributionList, nil),
	}

	if sd.recordTransmissionExecutionCarbon {
		result["average_execution_carbon"] = stat.Mean(executionCarbonList, nil)
		result["average_transmission_carbon"] = stat.Mean(transmissionCarbonList, nil)
	}

	return result
}

func (sd *SimpleDeploymentMetricsCalculator) CalculateDeploymentMetrics(dataString string) map[string]float64 {
	var data []interface{}
	err := json.Unmarshal([]byte(dataString), &data)
	if err != nil {
		fmt.Println("Error unmarshaling JSON:", err)
		return nil
	}
	deployment := make([]int, len(data))
	for i, d := range data {
		deployment[i] = int(d.(float64))
	}
	return sd.PerformMonteCarloSimulation(deployment)
}

func (sd *SimpleDeploymentMetricsCalculator) UpdateDataForNewHour(data string) string {
	sd.inputManager.AlterCarbonSetting(&data)
	return "void"
}
