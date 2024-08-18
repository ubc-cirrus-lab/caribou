package loaders

import "caribou-go/src/utils"

const SolverInputGridCarbonDefault = 500.0

type CarbonLoader struct {
	CarbonData map[string]interface{}
}

func NewCarbonLoader(carbonData map[string]interface{}) (*CarbonLoader, error) {
	carbonLoader := CarbonLoader{carbonData}
	return &carbonLoader, nil
}

func (c *CarbonLoader) GetTransmissionDistance(fromRegionName string, toRegionName string) float64 {
	return utils.Get(c.CarbonData, -1.0, fromRegionName, "transmission_distances", toRegionName)
}

func (c *CarbonLoader) GetGridCarbonIntensity(regionName string, hour *string) float64 {
	var carbonPolicy string
	if hour == nil {
		carbonPolicy = "overall"
	} else {
		carbonPolicy = *hour
	}
	return utils.Get(c.CarbonData, SolverInputGridCarbonDefault, regionName, "averages", carbonPolicy, "carbon_intensity")
}
