package loaders

import (
	"caribou-go/src/utils"
)

const (
	SolverInputRelativePerformanceDefault = 1.0
	SolverInputTransmissionLatencyDefault = 1000.0
)

type PerformanceLoader struct {
	PerformanceData map[string]interface{}
}

func NewPerformanceLoader(performanceData map[string]interface{}) (*PerformanceLoader, error) {
	performanceLoader := PerformanceLoader{PerformanceData: performanceData}
	return &performanceLoader, nil
}

func (p *PerformanceLoader) GetRelativePerformance(regionName string) float64 {
	return utils.Get(p.PerformanceData, SolverInputRelativePerformanceDefault, regionName, "relative_performance")
}

func (p *PerformanceLoader) GetTransmissionLatencyDistribution(fromRegionName string, toRegionName string) []float64 {
	return utils.GetList(p.PerformanceData, []float64{SolverInputTransmissionLatencyDefault}, fromRegionName, "transmission_latency", toRegionName, "latency_distribution")
}
