package calculators

import (
	"fmt"
	"strings"

	"caribou-go/src/deployment-input/components/loaders"
)

const AverageUsaCarbonIntensity = 410.0

type CarbonCalculator struct {
	CarbonLoader                      *loaders.CarbonLoader
	DatacenterLoader                  *loaders.DataCenterLoader
	WorkflowLoader                    *loaders.WorkflowLoader
	ConsiderCfe                       bool
	executionConversionRatioCache     map[string][]float64
	transmissionConversionRatioCache  map[string]float64
	hourlyCarbonSetting               *string
	EnergyFactor                      float64
	CarbonFreeIntraRegionTransmission bool
	CarbonFreeDtExecutionHomeRegion   bool
}

func NewCarbonCalculator(
	CarbonLoader *loaders.CarbonLoader,
	DatacenterLoader *loaders.DataCenterLoader,
	WorkflowLoader *loaders.WorkflowLoader,
	ConsiderCfe bool,
	EnergyFactorTrans float64,
	CarbonFreeIntraRegionTransmission bool,
	CarbonFreeDtExecutionHomeRegion bool,
) (*CarbonCalculator, error) {
	cc := CarbonCalculator{
		CarbonLoader:                      CarbonLoader,
		DatacenterLoader:                  DatacenterLoader,
		WorkflowLoader:                    WorkflowLoader,
		ConsiderCfe:                       ConsiderCfe,
		executionConversionRatioCache:     make(map[string][]float64),
		transmissionConversionRatioCache:  make(map[string]float64),
		hourlyCarbonSetting:               nil,
		EnergyFactor:                      EnergyFactorTrans,
		CarbonFreeIntraRegionTransmission: CarbonFreeIntraRegionTransmission,
		CarbonFreeDtExecutionHomeRegion:   CarbonFreeDtExecutionHomeRegion,
	}
	return &cc, nil
}

func (cc *CarbonCalculator) AlterCarbonSetting(carbonSetting *string) {
	cc.hourlyCarbonSetting = carbonSetting
	cc.executionConversionRatioCache = make(map[string][]float64)
	cc.transmissionConversionRatioCache = make(map[string]float64)
}

func (cc *CarbonCalculator) CalculateVirtualStartInstanceCarbon(
	dataInputSizes map[string]float64,
	dataOutputSizes map[string]float64,
) float64 {
	transmissionCarbon := 0.0

	// Even if the function is not invoked, we model
	// Each node as an abstract instance to consider
	// data transfer carbon
	transmissionCarbon += cc.CalculateDataTransferCarbon("", dataInputSizes, dataOutputSizes, 0.0)

	return transmissionCarbon
}

func (cc *CarbonCalculator) CalculateInstanceCarbon(
	executionTime float64,
	instanceName string,
	regionName string,
	dataInputSizes map[string]float64,
	dataOutputSizes map[string]float64,
	dataTransferDuringExecution float64,
	isInvoked bool,
	isDirector bool,
) (float64, float64) {
	executionCarbon := 0.0
	transmissionCarbon := 0.0

	// If the function is actually invoked
	if isInvoked {
		// Calculate the carbon from running the execution
		executionCarbon += cc.CalculateExecutionCarbon(instanceName, regionName, executionTime, isDirector)
	}

	// Even if the function is not invoked, we model
	// Each node as an abstract instance to consider
	// data transfer carbon
	transmissionCarbon += cc.CalculateDataTransferCarbon(regionName, dataInputSizes, dataOutputSizes, dataTransferDuringExecution)

	return executionCarbon, transmissionCarbon
}

func (cc *CarbonCalculator) CalculateDataTransferCarbon(
	currentRegionName string,
	dataInputSizes map[string]float64,
	dataOutputSizes map[string]float64,
	dataTransferDuringExecution float64,
) float64 {
	totalTransmissionCarbon := 0.0
	averageCarbonIntensityOfUsa := AverageUsaCarbonIntensity

	// Since the energy factor of transmission denotes the energy consumption
	// of the data transfer from and to destination, we do not want to double count.
	// Thus we can simply take a look at the data_input_sizes and ignore the data_output_sizes.
	dataTransferAccountedByWrapper := dataInputSizes
	for fromRegionName, dataTransferGb := range dataTransferAccountedByWrapper {

		transmissionNetworkCarbonIntensity := averageCarbonIntensityOfUsa
		// If consider_home_region_for_transmission is true,
		// then we consider there are transmission carbon EVEN for
		// data transfer within the same region.
		// Otherwise, we skip the data transfer within the same region
		if fromRegionName == currentRegionName {
			// If its intra region transmission, and if we
			// want to consider it as free, then we skip it.
			if cc.CarbonFreeIntraRegionTransmission {
				continue
			}
			if len(currentRegionName) != 0 {
				// Get the carbon intensity of the region (if known)
				// (If data transfer is within the same region)
				// Otherwise it will be inter-region data transfer,
				// and thus we use the average carbon intensity of the USA.
				transmissionNetworkCarbonIntensity = cc.CarbonLoader.GetGridCarbonIntensity(
					currentRegionName, cc.hourlyCarbonSetting,
				)
			}
		} else if len(fromRegionName) != 0 && len(currentRegionName) != 0 {
			// If we know the source and destination regions, we can get the carbon intensity
			// of the transmission network between the two regions.
			transmissionNetworkCarbonIntensity = cc.GetNetworkCarbonIntensityRouteBetweenRegions(
				fromRegionName, currentRegionName,
			)
		}
		totalTransmissionCarbon += dataTransferGb * cc.EnergyFactor * transmissionNetworkCarbonIntensity
	}

	// Calculate the carbon from data transfer
	// Of data that we CANNOT track represented by data_transfer_during_execution
	// There are no way to tell where the data is coming from
	// This may come from the data transfer of user code during execution OR
	// From Lambda runtimes or some AWS internal data transfer.
	currentRegionIsHomeRegion := currentRegionName == cc.WorkflowLoader.HomeRegion

	// We assume that half of the data transfer is from the home region
	// and the other half is from the average carbon intensity of the USA.
	homeRegionDtde, internetDtde := dataTransferDuringExecution/2, dataTransferDuringExecution/2

	// If the data transfer is from the internet, we use the average carbon intensity of the USA
	// And it is always consider inter-region data transfer. (So always apply)
	totalTransmissionCarbon += internetDtde * cc.EnergyFactor * averageCarbonIntensityOfUsa

	// If the data transfer is from the home region, we use the carbon intensity of the home region
	// And it is always consider intra-region data transfer. (May or may not apply)
	if !cc.CarbonFreeDtExecutionHomeRegion || !currentRegionIsHomeRegion {
		transmissionNetworkCarbonIntensity := averageCarbonIntensityOfUsa
		if len(currentRegionName) != 0 {
			setting := ""
			if cc.hourlyCarbonSetting != nil {
				setting = *cc.hourlyCarbonSetting
			}
			transmissionNetworkCarbonIntensity = cc.GetNetworkCarbonIntensityRouteBetweenRegions(
				currentRegionName, setting,
			)
		}

		totalTransmissionCarbon += homeRegionDtde * cc.EnergyFactor * transmissionNetworkCarbonIntensity
	}

	return totalTransmissionCarbon
}

func (cc *CarbonCalculator) GetNetworkCarbonIntensityRouteBetweenRegions(regionOne string, regionTwo string) float64 {
	if regionOne == regionTwo && len(regionOne) != 0 {
		regionOneCarbonIntensity := cc.CarbonLoader.GetGridCarbonIntensity(
			regionOne, cc.hourlyCarbonSetting,
		)
		return regionOneCarbonIntensity
	}

	// Get the carbon intensity of the route betweem two regions.
	// We can estimate it as the average carbon intensity of the grid
	// between the two regions. (No order is assumed)
	// If we have a better model, we can replace this with that.
	regionOneCarbonIntensity := cc.CarbonLoader.GetGridCarbonIntensity(
		regionOne, cc.hourlyCarbonSetting,
	)
	regionTwoCarbonIntensity := cc.CarbonLoader.GetGridCarbonIntensity(
		regionTwo, cc.hourlyCarbonSetting,
	)
	transmissionNetworkCarbonIntensityGco2e := (regionOneCarbonIntensity + regionTwoCarbonIntensity) / 2

	return transmissionNetworkCarbonIntensityGco2e
}

func (cc *CarbonCalculator) CalculateExecutionCarbon(instanceName string, regionName string, executionLatencyS float64, isRedirector bool) float64 {
	executionConversionRatio := cc.GetExecutionConversionRatio(instanceName, regionName, isRedirector)
	computeFactorKwH := executionConversionRatio[0]
	memoryFactorKwH := executionConversionRatio[1]
	powerFactorGco2eKwH := executionConversionRatio[2]

	cloudProviderUsageKwh := executionLatencyS * (computeFactorKwH + memoryFactorKwH)

	return cloudProviderUsageKwh * powerFactorGco2eKwH
}

func (cc *CarbonCalculator) GetExecutionConversionRatio(instanceName string, regionName string, isRedirector bool) []float64 {
	cacheKey := fmt.Sprintf("%s_%s", instanceName, regionName)
	if conversion, err := cc.executionConversionRatioCache[cacheKey]; err {
		return conversion
	}


	// Get the average power consumption of the instance in the given region (kw_GB)
	averageMemPower := cc.DatacenterLoader.GetAverageMemoryPower(regionName)

	// Get the carbon free energy of the grid in the given region
	cfe := 0.0
	if cc.ConsiderCfe {
		cfe = cc.DatacenterLoader.GetCfe(regionName)
	}

	// Get the power usage effectiveness of the datacenter in the given region
	pue := cc.DatacenterLoader.GetPue(regionName)

	// Get the carbon intensity of the grid in the given region (gCO2e/kWh)
	gridCo2e := cc.CarbonLoader.GetGridCarbonIntensity(regionName, cc.hourlyCarbonSetting)

	// Get the number of vCPUs and Memory of the instance
	provider := strings.Split(regionName, ":")[0] // Get the provider from the region name
	vcpu := cc.WorkflowLoader.GetVCpu(instanceName, provider)
	memory := cc.WorkflowLoader.GetMemory(instanceName, provider)

	// Get the min/max cpu power (In units of kWh)
	minCpuPower := cc.DatacenterLoader.GetMinCpuPower(regionName)
	maxCpuPower := cc.DatacenterLoader.GetMaxCpuPower(regionName)

	// Covert memory in MB to GB
	memory = memory / 1024

	utilization := cc.WorkflowLoader.GetAverageCpuUtilization(instanceName, regionName, isRedirector)
	averageCpuPower := minCpuPower + utilization*(maxCpuPower-minCpuPower)

	computeFactor := averageCpuPower * vcpu / 3600
	memoryFactor := averageMemPower * memory / 3600
	powerFactor := (1 - cfe) * pue * gridCo2e

	// Add the conversion ratio to the cache
	cc.executionConversionRatioCache[cacheKey] = []float64{computeFactor, memoryFactor, powerFactor}
	return cc.executionConversionRatioCache[cacheKey]
}
